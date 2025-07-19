import asyncio
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
from typing import Optional, Dict, Any
import redis.asyncio as redis
import aio_pika
from aio_pika import Message, DeliveryMode
from panoramisk.fast_agi import Application, Request
import signal
import sys
from dotenv import load_dotenv

def initlogger(log_file, log_level):
    log_dir = os.path.dirname(log_file)

    if log_dir and not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            print(f"Could not create log directory {log_dir}: {e}")
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file, maxBytes=100 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(file_formatter)

    # Remove all old handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Cấu hình
class Config:
    load_dotenv()
    # Redis config
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    REDIS_DB = int(os.getenv("REDIS_DB", 0))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

    # RabbitMQ config
    RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
    RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
    RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "call.events")

    # FastAGI config
    FASTAGI_HOST = os.getenv("FASTAGI_HOST", "0.0.0.0")
    FASTAGI_PORT = int(os.getenv("FASTAGI_PORT", 4573))

    LOG_FILE = os.getenv("LOG_FILE", "logs/fastagi.log")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Metric key prefix
    METRIC_KEY = os.getenv("METRIC_KEY", "stats:calls")


# Singleton connection managers
class RedisManager:
    _instance = None
    _redis_client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisManager, cls).__new__(cls)
        return cls._instance

    async def connect(self):
        """Khởi tạo kết nối Redis với auto-reconnect"""
        if self._redis_client is None:
            try:
                redis_url = (
                    f"redis://{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}"
                )
                if Config.REDIS_PASSWORD:
                    redis_url = f"redis://:{Config.REDIS_PASSWORD}@{Config.REDIS_HOST}:{Config.REDIS_PORT}/{Config.REDIS_DB}"

                self._redis_client = redis.from_url(
                    redis_url,
                    socket_keepalive=True,
                    socket_keepalive_options={},
                    health_check_interval=30,
                    retry_on_timeout=True,
                    decode_responses=True,
                )

                # Test connection
                await self._redis_client.ping()
                logger.info("Redis connected")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise

    async def get_redis(self):
        """Lấy Redis client với auto-reconnect"""
        if self._redis_client is None:
            await self.connect()

        # Test connection and reconnect if needed
        try:
            await self._redis_client.ping()
        except Exception:
            logger.warning("Redis connection lost, reconnecting...")
            self._redis_client = None
            await self.connect()

        return self._redis_client

    async def close(self):
        """Đóng kết nối Redis"""
        if self._redis_client:
            await self._redis_client.aclose()
            self._redis_client = None
            logger.info("Redis connection closed")


class RabbitMQManager:
    _instance = None
    _connection = None
    _channel = None
    _exchange = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RabbitMQManager, cls).__new__(cls)
        return cls._instance

    async def connect(self):
        """Khởi tạo kết nối RabbitMQ với auto-reconnect"""
        try:
            connection_string = f"amqp://{Config.RABBITMQ_USER}:{Config.RABBITMQ_PASSWORD}@{Config.RABBITMQ_HOST}:{Config.RABBITMQ_PORT}{Config.RABBITMQ_VHOST}"
            self._connection = await aio_pika.connect_robust(
                connection_string,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=1)

            # Tạo exchange và queue
            self._exchange = await self._channel.declare_exchange(
                Config.RABBITMQ_EXCHANGE, aio_pika.ExchangeType.DIRECT, durable=True
            )

            logger.info("RabbitMQ connected")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def _ensure_customer_queue(self, customer_id: str):
        queue_name = f"{Config.RABBITMQ_EXCHANGE}.{customer_id}"
        routing_key = queue_name

        # Declare queue theo từng customer_id
        queue = await self._channel.declare_queue(queue_name, durable=True)
        await queue.bind(self._exchange, routing_key=routing_key)

    async def publish_event(self, event_data: Dict[str, Any]):
        """Gửi event lên RabbitMQ"""

        customer_id = event_data.get("customer_id")
        if not customer_id:
            raise ValueError("customer_id is required to publish message")

        routing_key = f"{Config.RABBITMQ_EXCHANGE}.{customer_id}"
        try:
            if not self._connection or self._connection.is_closed:
                await self.connect()

            await self._ensure_customer_queue(customer_id)

            message = Message(
                json.dumps(event_data, ensure_ascii=False).encode("utf-8"),
                delivery_mode=DeliveryMode.PERSISTENT,
                timestamp=datetime.now(),
                content_type="application/json",
            )

            await self._exchange.publish(message, routing_key=routing_key)
            logger.info(f"Published to {routing_key}: {event_data}")
        except Exception as e:
            logger.error(f"Failed to publish event to RabbitMQ: {e}")
            # Thử reconnect và gửi lại
            try:
                await self.connect()
                message = Message(
                    json.dumps(event_data, ensure_ascii=False).encode("utf-8"),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    timestamp=datetime.now(),
                    content_type="application/json",
                )

                await self._exchange.publish(message, routing_key=routing_key)
                logger.info(f"Published to {routing_key}: {event_data}")
            except Exception as retry_error:
                logger.error(f"Failed to publish event after reconnect: {retry_error}")
                raise

    async def close(self):
        """Đóng kết nối RabbitMQ"""
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            logger.info("RabbitMQ connection closed")


# Event model
class CallEvent:
    def __init__(self, agi_data: Dict[str, Any]):
        self.timestamp = datetime.now().isoformat()
        self.call_id = agi_data.get("CALL_ID", "")
        self.direction = agi_data.get("DIRECTION", "").lower()
        self.customer_id = agi_data.get("CUSTOMER_ID") or "xxxx"
        self.trunk = agi_data.get("TRUNK", "")
        self.unique_id = agi_data.get("UNIQUE_ID", "")
        self.phone_number = agi_data.get("PHONE_DIAL", "")
        self.linked_id = agi_data.get("LINKED_ID", "")
        self.call_status = agi_data.get("CALL_STATUS", "").upper()
        self.queue_name = agi_data.get("QUEUE_NAME", "")
        self.bill_sec = agi_data.get("HANDLE_TIME", "") or "0"
        self.duration = agi_data.get("DURATION", "") or "0"
        self.start_time = agi_data.get("START_TIME", "")
        self.answer_time = agi_data.get("ANSWER_TIME", "") or "0"
        self.end_time = agi_data.get("END_TIME", "")
        self.agent_exten = agi_data.get("AGENT_EXTEN", "")
        self.agent_account = agi_data.get("AGENT_ACCOUNT", "")
        self.dial_mode = agi_data.get("DIAL_MODE", "")
        self.channel = agi_data.get("CHANNEL", "")

        # TTL for Redis keys (0.5 hour)
        self.ttl = 1800

    async def event_data(self):
        metrics = await self.get_all_metrics()
        event = {
            "timestamp": self.timestamp,
            "call_id": self.call_id,
            "direction": self.direction,
            "customer_id": self.customer_id,
            "trunk": self.trunk,
            "unique_id": self.unique_id,
            "phone_number": self.phone_number,
            "linked_id": self.linked_id,
            "call_status": self.call_status.lower(),
            "queue_name": self.queue_name,
            "bill_sec": self.bill_sec,
            "duration": self.duration,
            "start_time": self.start_time,
            "answer_time": (
                datetime.fromtimestamp(int(self.answer_time)).isoformat()
                if self.answer_time.isdigit() and int(self.answer_time) > 0
                else ""
            ),
            "end_time": self.end_time,
            "agent_exten": self.agent_exten,
            "agent_account": self.agent_account,
            "dial_mode": self.dial_mode,
            "channel": self.channel,
            "metrics": metrics,
        }

        return event

    async def get_all_metrics(self) -> Dict[str, int]:
        """Get current call counts for all statuses"""
        redis_manager = RedisManager()
        redis_client = await redis_manager.get_redis()
        metrics = {}
        statuses = ["ivr", "ring", "queue", "answer", "inbound", "outbound", "internal"]

        for status in statuses:
            key = f"{Config.METRIC_KEY}:{self.customer_id}:{status}"
            metrics[status] = await redis_client.scard(key)

        return metrics

    async def add_call_status(self, status: str) -> bool:
        """Add call to a specific status set"""
        try:
            redis_manager = RedisManager()
            redis_client = await redis_manager.get_redis()

            key = f"{Config.METRIC_KEY}:{self.customer_id}:{status.lower()}"
            await redis_client.sadd(key, self.unique_id)
            await redis_client.expire(key, self.ttl)
            return True
        except Exception as e:
            print(f"Error adding call status: {e}")
            return False

    async def remove_call_from_statuses(self, statuses) -> bool:
        """Remove call from multiple status sets"""
        try:
            redis_manager = RedisManager()
            redis_client = await redis_manager.get_redis()
            for status in statuses:
                key = f"{Config.METRIC_KEY}:{self.customer_id}:{status.lower()}"
                await redis_client.srem(key, self.unique_id)
            return True
        except Exception as e:
            print(f"Error removing call from statuses: {e}")
            return False

    async def process_call_status(self) -> bool:
        """Process call status changes like the Go code using match-case"""
        try:
            match self.call_status:
                case "IVR" | "CALL" | "OUTBOUND":
                    match self.direction:
                        case "inbound":
                            await self.add_call_status("inbound")
                            await self.add_call_status("ivr")
                        case "outbound" | "internal":
                            await self.add_call_status(self.direction)

                case "RING" | "RINGING":
                    await self.add_call_status("ring")
                    await self.remove_call_from_statuses(["ivr"])

                case "QUEUE" | "ENTERQUEUE":
                    await self.add_call_status("queue")
                    await self.remove_call_from_statuses(["ivr"])

                case "ANSWER":
                    await self.add_call_status("answer")
                    await self.remove_call_from_statuses(["ring", "queue", "ivr"])

                case "HANGUP" | "HANGUPV9":
                    await self.remove_call_from_statuses(
                        [
                            "ivr",
                            "ring",
                            "queue",
                            "answer",
                            "inbound",
                            "outbound",
                            "internal",
                        ]
                    )

                case _:
                    # Default case - no action needed
                    pass

            return True

        except Exception as e:
            print(f"Error processing call status: {e}")
            return False


# AGI Handlers
class AGIHandler:
    def __init__(self):
        self.redis_manager = RedisManager()
        self.rabbitmq_manager = RabbitMQManager()

    async def checkphone(self, agi: Application):
        """Kiểm tra blacklist và VIP cho số điện thoại"""

        try:

            await agi.send_command(f"VERBOSE ======Start-check-phone-number=== 1")
            phone_number = agi.headers.get("agi_callerid", "").strip()
            caller_number = result = await agi.send_command(f"GET VARIABLE PHONE_DIAL")

            if not phone_number and not caller_number:
                logger.warning("No phone number found in AGI variables")
                return

            check_phone = phone_number if phone_number else caller_number

            logger.info(f"Checking phone number: {check_phone}")

            redis_client = await self.redis_manager.get_redis()

            # Kiểm tra blacklist
            blacklist_key = f"phone:blacklist:{check_phone}"
            is_blacklisted = await redis_client.exists(blacklist_key)

            if is_blacklisted:
                logger.info(f"Phone {check_phone} is blacklisted, hangup")

                await agi.send_command(f"VERBOSE ======Phone-blacklisted=Hangup 1")
                await agi.send_command("HANGUP")
                return

            # Kiểm tra VIP
            vip_key = "phone:vip"
            is_vip = await redis_client.sismember(vip_key, check_phone)

            if is_vip:
                logger.info(f"Phone {check_phone} is VIP, setting high priority")
                await agi.send_command("SET VARIABLE QUEUE_PRIO 10")
                await agi.send_command(f"VERBOSE ======Phone-{check_phone}-is-VIP 1")
            else:
                logger.info(f"Phone {check_phone} is regular caller")
                await agi.send_command(f"VERBOSE ======Normal-caller:{check_phone} 1")

            logger.info(f"Checkphone completed for {check_phone}")

        except Exception as e:
            logger.error(f"Error in checkphone handler: {e}")

    async def event(self, agi: Application):

        try:
            # logger.info(f"Received event request: {agi.headers}")
            await agi.send_command(f"VERBOSE ======Send-Call-Event=== 1")

            agi_vars = [
                "START_TIME",
                "CALL_ID",
                "DIRECTION",
                "CUSTOMER_ID",
                "TRUNK",
                "UNIQUE_ID",
                "PHONE_DIAL",
                "LINKED_ID",
                "CALL_STATUS",
                "QUEUE_NAME",
                "ANSWER_TIME",
                "END_TIME",
                "AGENT_EXTEN",
                "AGENT_ACCOUNT",
                "DIAL_MODE",
                "HANDLE_TIME",
                "CHANNEL",
                "DURATION",
            ]
            agi_data = {}
            for var in agi_vars:
                var1 = None
                match var:
                    case "START_TIME":
                        var1 = "CDR(start)"
                    case "ANSWER_TIME":
                        var1 = f"SHARED(ANSWER_TIME,{agi_data['CALL_ID']})"
                    case "AGENT_EXTEN":
                        var1 = f"SHARED(AGENT_EXTEN,{agi_data['CALL_ID']})"
                    case "AGENT_ACCOUNT":
                        var1 = f"SHARED(AGENT_ACCOUNT,{agi_data['CALL_ID']})"
                    case "DURATION":
                        var1 = "CDR(duration)"
                    case "END_TIME":
                        var1 = "CDR(end)"
                    case _:
                        var1 = var
                result = await agi.send_command(f"GET VARIABLE {var1}")
                value = ""
                if isinstance(result, dict) and "result" in result:
                    value = result["result"][1] if len(result["result"]) > 1 else ""
                agi_data[var] = value
            # logger.info(f"AGI data received: {agi_data}")
            call_event = CallEvent(agi_data)
            await call_event.process_call_status()
            event_data = await call_event.event_data()

            # Gửi event lên RabbitMQ
            await self.rabbitmq_manager.publish_event(event_data)
            logger.info(f"Event handler completed.")
            await agi.send_command(f"VERBOSE ======Event-handler-completed=== 1")

        except Exception as e:
            logger.error(f"Error in event handler: {e}")


# FastAGI Server
class FastAGIApp:
    def __init__(self):
        self.server = None
        self.redis_manager = RedisManager()
        self.rabbitmq_manager = RabbitMQManager()
        self.agi_handler = AGIHandler()

    async def setup_connections(self):
        """Khởi tạo tất cả kết nối"""
        try:
            logger.info("Setting up connections...")
            await self.redis_manager.connect()
            await self.rabbitmq_manager.connect()
            logger.info("All connections established successfully")
        except Exception as e:
            logger.error(f"Failed to setup connections: {e}")
            raise

    async def start_server(self):
        """Khởi động FastAGI server"""
        try:
            # Khởi tạo kết nối
            await self.setup_connections()

            # Tạo server
            self.server = Application()

            # Đăng ký routes
            self.server.add_route("/calls/checkphone", self.agi_handler.checkphone)
            self.server.add_route("calls/checkphone", self.agi_handler.checkphone)
            self.server.add_route("/calls/event", self.agi_handler.event)
            self.server.add_route("calls/event", self.agi_handler.event)
            logger.info(
                f"FastAGI server starting on {Config.FASTAGI_HOST}:{Config.FASTAGI_PORT}"
            )
            server = await asyncio.start_server(
                self.server.handler, Config.FASTAGI_HOST, Config.FASTAGI_PORT
            )
            await server.serve_forever()
            # await self.server.start()

        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            raise

    async def stop_server(self):
        """Dừng server và đóng kết nối"""
        try:
            if self.server:
                await self.server.stop()
                logger.info("FastAGI server stopped")

            await self.redis_manager.close()
            await self.rabbitmq_manager.close()
            logger.info("All connections closed")

        except Exception as e:
            logger.error(f"Error stopping server: {e}")


# Global app instance
app = FastAGIApp()
logger = None


async def main():

    global logger
    logger = initlogger(Config.LOG_FILE, Config.LOG_LEVEL)

    # Setup signal handlers
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down...")
        asyncio.create_task(app.stop_server())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await app.start_server()

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        await app.stop_server()


if __name__ == "__main__":
    asyncio.run(main())
