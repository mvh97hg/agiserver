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
Key: phone:blacklist:{PhoneNumber}
Value: "1"
TTL: 86400 seconds (24 hours)
```

#### VIP List
```
Key: phone:vip
Type: Set
Members: PhoneNumber
```
#### Trạng thái cuộc gọi
```
Key stats:calls:{CustomerId}:{CallStatus}
Type: Set
Members: CallId
```

### RabbitMQ

```
Exchange: call.events
Queue: call.events.{CustomerId}
Routing key: call.events.{CustomerId}
```
#### Event JSON Model

```json
{
  "timestamp": "2025-07-19T03:37:15.615216",
  "CallId": "1752896235.284",
  "Direction": "inbound",
  "CustomerID": "11223344",
  "Trunk": "0866801898",
  "UniqueID": "1752896235.284",
  "PhoneNumber": "84912406496",
  "LinkedID": "1752896235.284",
  "CallStatus": "IVR",
  "QueueName": "ky_thuat",
  "BillSec": "0",
  "Duration": "0",
  "StartTime": "2025-07-19 10:37:15",
  "AnswerTime": "",
  "EndTime": "",
  "AgentExten": "NONE",
  "AgentAccount": "NONE",
  "DialMode": "",
  "Channel": "PJSIP/0866801898-000000bb",
  "metrics": {
    "ivr": 1,
    "ring": 0,
    "queue": 0,
    "answer": 0,
    "inbound": 1,
    "outbound": 0,
    "internal": 0
  }
}
```
