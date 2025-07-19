# FastAGI Server với Redis và RabbitMQ

FastAGI server sử dụng Python với thư viện `panoramisk.fast_agi` để xử lý các cuộc gọi từ Asterisk, tích hợp Redis để quản lý blacklist/VIP và RabbitMQ để gửi events.

## Tính năng chính

- **Route `/checkphone`**: Kiểm tra blacklist và VIP cho số điện thoại
- **Route `/event`**: Gửi thông tin cuộc gọi lên RabbitMQ
- **Redis**: Lưu trữ blacklist và VIP list
- **RabbitMQ**: Gửi events cuộc gọi dưới dạng JSON
- **Auto-reconnect**: Tự động kết nối lại khi mất kết nối
- **Logging**: Ghi log đầy đủ vào file và console

## Yêu cầu hệ thống

- Python 3.9+
- Redis Server
- RabbitMQ Server
- Asterisk

## Cài đặt

1. **Clone và cài đặt dependencies**:
```bash
pip install -r requirements.txt
```

2. **Cấu hình environment variables**:
```bash
cp .env.example .env
# Chỉnh sửa file .env
```

## Cấu hình Asterisk

Thêm vào file `/etc/asterisk/extensions.conf`:

```ini
[default]
; Kiểm tra blacklist/VIP
exten => _X.,1,AGI(agi://localhost:4573/checkphone)
exten => _X.,n,Dial(SIP/${EXTEN})

; Gửi event
exten => _X.,1,AGI(agi://localhost:4573/event)
exten => _X.,n,Hangup()
```
## Các biến dialplan
```
  CALL_ID
  DIRECTION
  CUSTOMER_ID
  TRUNK
  UNIQUE_ID
  PHONE_DIAL
  LINKED_ID
  CALL_STATUS
  QUEUE_NAME
  HANDLE_TIME
  SHARED(ANSWER_TIME,${CALL_ID})
  SHARED(AGENT_EXTEN,${CALL_ID})
  SHARED(AGENT_ACCOUNT,${CALL_ID})
```
## Cấu trúc dữ liệu
### Redis
#### Blacklist
```
Key: phone:blacklist:{phone_number}
Value: "1"
TTL: 86400 seconds (24 hours)
```

#### VIP List
```
Key: phone:vip
Type: Set
Members: phone_number
```
#### Trạng thái cuộc gọi
```
Key stats:calls:{customer_id}:{call_status}
Type: Set
Members: CallId
```

### RabbitMQ

```
Exchange: call.events
Queue: call.events.{customer_id}
Routing key: call.events.{customer_id}
```
#### Event JSON Model

```json
{
  "timestamp": "2025-07-19T04:31:21.262589",
  "call_id": "1752899481.313",
  "direction": "inbound",
  "customer_id": "11223344",
  "trunk": "0866801898",
  "unique_id": "1752899481.313",
  "phone_number": "84912406496",
  "linked_id": "1752899481.313",
  "call_status": "ringing",
  "queue_name": "ky_thuat",
  "bill_sec": "0",
  "duration": "0",
  "start_time": "2025-07-19 11:31:21",
  "answer_time": "",
  "end_time": "",
  "agent_exten": "1001",
  "agent_account": "dh02",
  "dial_mode": "",
  "channel": "PJSIP/1001-000000ce",
  "metrics": {
    "ivr": 0,
    "ring": 1,
    "queue": 1,
    "answer": 0,
    "inbound": 1,
    "outbound": 0,
    "internal": 0
  }
}

```
