# Phần Common — Ý tưởng, cách vận hành, kịch bản thuyết trình

**IT4062 — Lập trình mạng · Đề tài 16 · Publish/Subscribe Framework**

> Tài liệu này trả lời 3 câu: **ý tưởng ở đâu ra**, **nó vận hành thế nào**, và **nói gì trước lớp**.
> Phần mô tả code chi tiết nằm trong `COMMON_MODULE.md`; đây là bản để *hiểu và trình bày*.

---

## PHẦN A — Ý TƯỞNG Ở ĐÂU RA

Đây là câu hỏi quan trọng nhất khi bảo vệ. Giáo viên không hỏi "em viết hàm gì" mà hỏi **"vì sao em phải viết nó"**. Toàn bộ module `common/` không phải sáng kiến riêng, mà là **áp dụng ba nguyên lý đã chuẩn hoá trong công nghiệp mạng**. Nắm được nguồn gốc là bạn trả lời được mọi câu hỏi "tại sao".

### A.1 Ý tưởng 1 — Đóng khung thông điệp (message framing)

**Vấn đề gốc nằm trong định nghĩa của TCP.** TCP được thiết kế như một **dòng byte liên tục** (byte stream), không phải dãy gói tin. RFC 9293 (bản mới của RFC 793) nói rõ: TCP đảm bảo byte đến **đúng thứ tự và đầy đủ**, nhưng **không** đảm bảo ranh giới giữa các lần `write()` của bên gửi còn được giữ ở bên nhận.

Hệ quả thực tế — hai hiện tượng có tên riêng:

| Hiện tượng | Nghĩa | Ví dụ |
|---|---|---|
| **Fragmentation** (phân mảnh) | 1 lần gửi → nhiều lần nhận | `write(36 byte)` → `read()` được 8, rồi 28 |
| **Coalescing** (gộp) | nhiều lần gửi → 1 lần nhận | 5 `write(36)` → 1 `read()` 180 byte |

Nếu code làm `read(fd, &header, 12)` rồi tin ngay đó là header → sai ngay khi mạng bận. **Đây chính là lý do tồn tại của `protocol.c` + `buffer.c`.**

**Giải pháp: length-prefixed framing** — gửi kèm độ dài trước nội dung, bên nhận đọc đủ `length` byte mới xử lý. Đây là pattern phổ quát, có mặt trong hầu hết giao thức lớn:

| Giao thức | Header | Cách đánh dấu độ dài |
|---|---|---|
| **TLS** (RFC 8446) | 5 byte | type(1) + version(2) + **length(2)** |
| **WebSocket** (RFC 6455) | 2–14 byte | FIN/opcode + **payload length** (7/16/64 bit) |
| **MQTT** | 2+ byte | type(1) + **remaining length** (varint) |
| **HTTP/1.1** | dòng text | header `Content-Length` |
| **Kafka** | 4 byte | **size prefix** |
| **Redis RESP** | text | `$<len>\r\n<payload>` |
| **Đồ án này** | **12 byte** | magic(2) + ver(1) + type(1) + topic_len(2) + status(2) + **payload_len(4)** |

→ Câu nói chuẩn khi bảo vệ: *"Header 12 byte của em là biến thể của **TLS record header** và **WebSocket frame header**: cùng một ý tưởng là đặt trường độ dài lên trước, nhưng em thêm `topic_len` vì đây là giao thức pub/sub chứ không phải stream thuần."*

**Vì sao có `magic`?** Lấy từ ý tưởng **"magic number"** dùng để nhận diện định dạng file/giao thức (ELF `\x7fELF`, PNG `\x89PNG`, gzip `\x1f\x8b`). Ở đây `0x5053` = ASCII `"PS"` giúp phát hiện ngay khi bên nhận **mất đồng bộ khung** — nếu không có nó, một `payload_len` rác có thể khiến server `malloc` vài GB.

### A.2 Ý tưởng 2 — Byte order chuẩn hoá (network byte order)

Máy Intel/AMD (x86) là **little-endian**: số `0x5053` nằm trong RAM là `53 50`. Nhưng chuẩn mạng là **big-endian**, quy định từ **RFC 1700** ("network byte order"). Vì vậy C cung cấp `htons` / `htonl` / `ntohs` / `ntohl` (`hto` = *host to network*).

Nếu quên bước này, server và client cùng chạy trên x86 thì **vẫn chạy đúng** (vì cùng sai), nhưng:
- chạy client trên máy ARM big-endian → sai,
- hoặc khi bị chấm trên môi trường khác → sai,
- và quan trọng hơn: **sai về mặt thiết kế giao thức**.

→ Câu nói chuẩn: *"Em luôn `to_network` trước khi gửi và `to_host` sau khi nhận. Test có một case round-trip `to_network → to_host` để chứng minh không mất dữ liệu, và test `sizeof(frame_header_t) == 12` để chứng minh `#pragma pack(1)` chặn được padding."*

**Vì sao cần `#pragma pack(1)`?** Compiler chèn byte đệm để căn lề field cho CPU truy cập nhanh. Struct 12 byte ở trên có thể thành 16 byte nếu không pack → hai máy hiểu khác nhau → giao thức sụp. Đây là lỗi kinh điển khi gửi struct qua mạng.

### A.3 Ý tưởng 3 — Bộ đệm tích luỹ (accumulating buffer)

Ý tưởng này lấy từ **cách mọi thư viện mạng đều làm**:

| Nơi khác | Tên gọi |
|---|---|
| Nhân Linux | `sk_buff`, `kfifo` |
| Java Netty | `ByteBuf` (readerIndex / writerIndex) |
| Go | `bufio.Reader` |
| Node.js | `Buffer` + `StringDecoder` |
| Đồ án này | `byte_buffer_t` (read_pos / write_pos) |

Trong tất cả các thiết kế đó đều có **hai con trỏ**: một đánh dấu "đã xử lý đến đâu", một đánh dấu "đã ghi đến đâu". `readable = write_pos - read_pos` là lượng dữ liệu chưa xử lý. Đây chính là mô hình **producer–consumer** cổ điển.

Em chọn biến thể **linear buffer + compaction** thay vì **ring buffer** vì lý do rất cụ thể: ring buffer khi frame bị "quấn vòng" qua cuối mảng thì phải parse thành 2 nhánh (phần đuôi + phần đầu), là nguồn bug lớn. Với linear buffer, dữ liệu của một frame **luôn liền mạch** → parse chỉ cần một `memcpy`. Chi phí `memmove` khi compact rất nhỏ so với lợi ích về độ đúng đắn.

Và cơ chế **tăng gấp đôi capacity** là kỹ thuật **amortized O(1)** — giống hệt cách `std::vector` (C++) hay `ArrayList` (Java) hoạt động: không cấp phát lại mỗi lần thêm 1 byte, mà nhân đôi, nên trung bình mỗi lần thêm chỉ tốn O(1).

### A.4 Ý tưởng 4 — Non-blocking I/O và epoll

Bài toán **C10K** (một server phục vụ 10.000 kết nối) là động lực ra đời của `epoll` trên Linux. Với `select`/`poll`, mỗi lần chờ phải quét toàn bộ danh sách fd → O(n). `epoll` dùng cấu trúc dữ liệu trong nhân, chỉ trả về các fd **thực sự có sự kiện** → O(số fd ready).

Nhưng `epoll` chỉ báo **"sẵn sàng"**, không đảm bảo **"đủ dữ liệu cho một lần đọc trọn vẹn"**. Vì vậy mọi socket phải **non-blocking** (`O_NONBLOCK`): nếu `read()` không có dữ liệu thì trả `EAGAIN` ngay thay vì treo thread. Server đơn luồng mà một `read()` blocking là **toàn bộ broker đứng hình** — đây chính là lý do `set_nonblocking()` nằm trong `common/` chứ không phải trong server.

`TCP_NODELAY` thì ngược lại: **tắt** Nagle (thuật toán gộp gói nhỏ của TCP để tiết kiệm băng thông). Broker ưu tiên **latency** hơn băng thông, nên tắt Nagle để mỗi `DELIVER` đi ngay.

### A.5 Tóm gọn phần "ý tưởng ở đâu"

> "Module `common/` của em không phát minh gì mới — nó **hiện thực hoá ba nguyên lý đã chuẩn hoá**: (1) TCP là byte stream nên phải tự đóng khung, em dùng **length-prefixed header** giống TLS/WebSocket/MQTT; (2) số nhiều byte phải theo **network byte order** theo RFC 1700; (3) phải có **bộ đệm tích luỹ** để ghép mảnh, giống `ByteBuf` của Netty hay `sk_buff` của nhân Linux. Điểm riêng của đồ án là em thêm `topic_len` vào header để phục vụ mô hình pub/sub."

---

## PHẦN B — CÁCH VẬN HÀNH

### B.1 Ba module, ba trách nhiệm tách bạch

```
                 ┌──────────────────────────────────────┐
   ứng dụng      │  server/  và  client/                │
   (nghiệp vụ)   │  "gửi cái gì, cho ai, khi nào"       │
                 └───────────────┬──────────────────────┘
                                 │ gọi hàm
                 ┌───────────────▼──────────────────────┐
   common/       │  protocol.c   "1 frame trông thế nào"│
   (hạ tầng)     │  buffer.c     "ghép byte thành frame"│
                 │  network_utils.c  "socket thế nào"   │
                 └───────────────┬──────────────────────┘
                                 │ system call
                 ┌───────────────▼──────────────────────┐
   kernel        │  TCP/IP stack, epoll                 │
                 └──────────────────────────────────────┘
```

Nguyên tắc: **nghiệp vụ không được biết đến byte order, không được gọi `malloc` cho buffer, không được gọi `fcntl`**. Nếu bạn làm server mà phải viết `htons` trong `server_epoll.c` → đã vi phạm phân lớp.

### B.2 Vận hành khi GỬI một tin (đường đi của 1 frame)

```
 [publisher.c]
   1. frame_header_init(&hdr, MSG_PUBLISH, 13, 200, 11)   ← giá trị "người đọc được"
        hdr = { magic=0x5053, ver=1, type=7, topic_len=13, status=200, payload_len=11 }
   2. frame_header_to_network(&hdr, &wire)                 ← htons/htonl
        wire trên RAM = 50 53 01 07 00 0D 00 C8 00 00 00 0B
   3. memcpy header + topic + payload vào 1 mảng 36 byte
   4. write(sock_fd, buf, 36)
```

Điểm cần nhấn: bước 2 là **bắt buộc**, và nó chỉ nằm ở **một chỗ duy nhất** trong toàn dự án.

### B.3 Vận hành khi NHẬN (vòng lặp quan trọng nhất của đồ án)

Đây là vòng lặp mà mọi server/client đều dùng, và là thứ `buffer.c` sinh ra để phục vụ:

```c
for (;;) {
    /* (1) Đọc thêm byte vào buffer — BAO NHIÊU CŨNG ĐƯỢC */
    buffer_ensure_writable(rx, 4096);
    ssize_t n = read(fd, buffer_write_ptr(rx), 4096);
    if (n > 0) buffer_commit_write(rx, (size_t)n);
    else if (n < 0 && errno == EAGAIN) break;      /* chờ epoll lần sau */

    /* (2) Parse được bao nhiêu frame thì parse bấy nhiêu */
    while (buffer_readable_bytes(rx) >= FRAME_HEADER_SIZE) {

        memcpy(&wire, buffer_read_ptr(rx), 12);
        frame_header_to_host(&wire, &hdr);          /* về host order */
        if (!frame_header_validate(&hdr)) -> đóng kết nối;

        size_t need = 12 + hdr.topic_len + hdr.payload_len;

        if (buffer_readable_bytes(rx) < need)
            break;                                  /* ← THIẾU BYTE: chờ thêm */

        /* (3) ĐỦ frame → xử lý nghiệp vụ */
        handle_frame(&hdr, buffer_read_ptr(rx) + 12);

        /* (4) Giải phóng phần đã dùng */
        buffer_consume(rx, need);
    }
}
```

**Ba dòng quan trọng nhất để chỉ khi thuyết trình:**

1. `while (readable >= 12)` — đủ header mới đọc header.
2. `if (readable < need) break;` — **chưa đủ payload thì dừng, không block, không lỗi**. Đây chính là chỗ xử lý fragmentation.
3. `while` (không phải `if`) — xử lý được coalescing: một lần `read()` có 5 frame thì parse cả 5.

### B.4 Trạng thái của buffer qua thời gian (dễ hình dung nhất)

Với frame 36 byte đến từng byte một:

```
sau read #1:  [50]                                     readable=1   → chưa đủ 12, chờ
sau read #5:  [50 53 01 07 00]                          readable=5   → chờ
sau read #12: [50 53 01 07 00 0D 00 C8 00 00 00 0B]     readable=12  → có header!
                                                                      need = 12+13+11 = 36
sau read #20: [... 20 byte ...]                         readable=20  → 20 < 36, chờ
sau read #36: [... 36 byte ...]                         readable=36  → ĐỦ → PARSE
              buffer_consume(36) → readable=0
```

Nếu gộp: một `read()` trả 72 byte = 2 frame → vòng `while` parse 2 lần, `consume` 2 lần.

### B.5 Khi buffer đầy thì chuyện gì xảy ra (cơ chế 2 tầng)

```
buffer_ensure_writable(buf, len):
   ┌─ đuôi còn đủ chỗ?  ──────────── có ──► dùng luôn (0 chi phí)
   │
   ├─ không → COMPACT: memmove phần readable về offset 0
   │           ┌─ giờ đủ chỗ? ──────── có ──► dùng (không cấp phát)
   │           │
   │           └─ vẫn không đủ → REALLOC, capacity × 2 (256→512→1024…)
   │                             └─ realloc fail → trả false (không crash)
```

Tầng compact là điểm nhiều bạn làm sai: nếu chỉ có realloc thì một luồng dữ liệu liên tục sẽ làm buffer phình mãi dù dữ liệu cũ đã parse xong → rò rỉ bộ nhớ kiểu "unbounded growth".

---

## PHẦN C — KỊCH BẢN NÓI (khoảng 7 phút)

### Mở đầu (30 giây)

> "Em phụ trách thư mục `common/`, gồm 3 file: `protocol.c`, `buffer.c`, `network_utils.c`.
> Đây là **lớp nền** dùng chung: cả broker lẫn publisher và subscriber đều link cùng 3 object file này. Nói cách khác, nếu phần của em sai thì cả hệ thống sai, vì cả ba chương trình đều nói chuyện qua đúng cái định dạng do em định nghĩa."

### Vấn đề (1 phút) — chỗ ghi điểm

> "Trước khi viết code, em phải giải quyết một vấn đề nằm trong bản chất của TCP: **TCP là byte stream, không phải message stream**. Nếu publisher `write()` ba lần liên tiếp, subscriber có thể `read()` một lần được cả ba frame gộp lại, hoặc ba lần mỗi lần nửa frame. TCP chỉ đảm bảo byte đến đúng thứ tự và đầy đủ, **không** đảm bảo ranh giới tin nhắn.
> Vì vậy em phải tự định nghĩa ranh giới. Cách em chọn là **length-prefixed framing** — đặt trường độ dài lên trước nội dung. Đây cũng chính là cách TLS, WebSocket, MQTT và Kafka làm; em chỉ thêm trường `topic_len` vì đây là giao thức pub/sub."

### Giải pháp 1 — protocol.c (1 phút 30)

> "Em thiết kế header **cố định 12 byte**:
> `magic 2 byte` = `0x5053` tức ASCII `'P','S'`, `version 1 byte`, `msg_type 1 byte`, `topic_len 2 byte`, `status_code 2 byte`, `payload_len 4 byte`.
>
> Ba quyết định em muốn giải thích:
> **Một**, `#pragma pack(1)` — vì struct C có thể bị chèn padding, không pack thì `sizeof` có thể là 16 trên máy khác, giao thức sẽ lệch. Test của em có assert `sizeof == 12`.
> **Hai**, mọi trường nhiều byte đều qua `htons`/`htonl` theo network byte order của RFC 1700. Bên gửi gọi `to_network`, bên nhận gọi `to_host`.
> **Ba**, có hàm `validate` chạy **trước khi** cấp phát: kiểm tra magic, version, msg_type hợp lệ, `topic_len ≤ 256`, `payload_len ≤ 1 MB`. Nếu client gửi `payload_len = 4 GB` mà em `malloc` trước rồi mới kiểm tra thì đó là lỗ hổng DoS."

### Giải pháp 2 — buffer.c (1 phút 30) — **đây là phần nên demo**

> "Giải pháp thứ hai là bộ đệm byte. Em dùng bố cục tuyến tính với hai con trỏ `read_pos` và `write_pos`; hiệu của chúng là số byte chưa parse.
>
> Em chọn **linear + compaction** thay vì ring buffer, vì ring buffer khi frame bị quấn vòng thì phải parse hai nhánh, rất dễ bug. Đổi lại em phải `memmove` khi compact, nhưng chi phí đó không đáng kể.
>
> Cơ chế mở rộng có hai tầng: nếu đuôi còn chỗ thì dùng luôn; nếu không thì compact về đầu; chỉ khi vẫn thiếu mới `realloc` và **nhân đôi** capacity — đây là kỹ thuật amortized O(1), giống `std::vector`.
>
> Em xin chạy demo để minh hoạ."

### Demo (1 phút)

```bash
make demo
```

> "Demo này gửi **một** frame 36 byte, nhưng chia thành **36 lần `write()` mỗi lần 1 byte**, và bên nhận `read()` mỗi lần 5 byte — tức là tệ nhất có thể.
> Các bạn thấy bên nhận đếm: lần 1 đến 11 thì `readable` chưa đủ 12 nên **chờ**; đến lần 12 thì có header, tính ra cần 36 byte; từ lần 13 đến 35 vẫn **chờ**; đến lần 36 thì `readable = 36` → parse ra đúng `magic 0x5053`, `msg_type = PUBLISH`, `topic = weather/hanoi`, `payload = {"temp":31}`.
> Điểm cần thấy: nếu đọc thẳng vào struct mà không có buffer, thì lần `read()` đầu chỉ có 5 byte là đã sai ngay."

### Giải pháp 3 — network_utils.c (45 giây)

> "File thứ ba là tiện ích socket. Bốn hàm quan trọng: `set_nonblocking` vì server đơn luồng nên một `read` blocking sẽ treo cả broker; `set_tcp_nodelay` vì broker ưu tiên latency hơn băng thông; `create_server_socket` có `SO_REUSEADDR` để restart server ngay sau crash mà không phải chờ `TIME_WAIT`; và `connect_to_server`. Cả hai hàm resolve địa chỉ bằng `getaddrinfo` với `AF_UNSPEC` nên chạy được cả IPv4 lẫn IPv6 mà không cần `#ifdef`."

### Kết (30 giây)

> "Tóm lại phần của em là **định nghĩa ngôn ngữ chung** của hệ thống: một frame trông thế nào, ghép byte rời thành frame ra sao, và socket được thiết lập thế nào. Em đã test 53 assertion, biên dịch với `-Wall -Wextra -Wpedantic -Werror` không có warning, và chạy dưới AddressSanitizer không rò rỉ bộ nhớ. Có phần test loopback thật: gửi một frame qua TCP rồi parse lại, so từng byte."

---

## PHẦN D — CHỨNG MINH (chạy tại chỗ)

```bash
make test      # 53/53 assertion pass
make demo      # minh hoạ fragmentation trực quan
make asan      # ASan + UBSan: không leak, không UB
```

Ba con số nên nhớ:

| Con số | Ý nghĩa |
|---|---|
| **12** | header cố định, `sizeof(frame_header_t)` có assert |
| **53** | số assertion trong unit test, tất cả pass |
| **0** | số warning khi build với `-Werror` |

---

## PHẦN E — CÂU HỎI KHÓ (chuẩn bị trước)

**1. Sao không dùng JSON/XML rồi gửi text cho đơn giản?**
Vì vẫn phải giải quyết ranh giới tin nhắn (JSON không tự biết kết thúc ở đâu khi ghép nhiều object), và parse text tốn CPU + không biểu diễn được dữ liệu nhị phân (ảnh, file). Header nhị phân cố định cho parse O(1) và chứa được payload nhị phân tuỳ ý.

**2. Sao không dùng Protobuf/MessagePack?**
Được, nhưng Protobuf vẫn cần framing bên ngoài (thường là 4-byte length prefix). Ở đồ án này header tự viết giúp thấy rõ bản chất vấn đề, và không phụ thuộc thư viện ngoài (yêu cầu của môn).

**3. Sao `payload_len` là 4 byte mà không phải 2?**
2 byte chỉ chứa tối đa 65.535 byte. Đồ án cho payload tới 1 MB nên cần 4 byte (tối đa 4 GB).

**4. Sao `magic` chỉ 2 byte mà không phải 4?**
2 byte đủ để xác suất trùng ngẫu nhiên là 1/65536, và tiết kiệm 2 byte trên **mỗi** frame — với broker gửi hàng triệu frame thì đáng kể. Nếu cần chắc chắn hơn có thể dùng 4 byte (như HTTP/2 dùng 24-bit length + 8-bit type).

**5. Nếu client gửi header hợp lệ nhưng không bao giờ gửi payload (tấn công slowloris)?**
Đây là hạn chế em biết: buffer sẽ chờ mãi. Bản đầy đủ cần **timeout** — ghi lại `last_active_at` và đóng kết nối nếu treo quá N giây. Cấu trúc `client_session_t` đã có trường `last_active_at` để làm việc này.

**6. Buffer có thread-safe không?**
Không, và không cần: thiết kế là **single-threaded epoll**, mỗi session chỉ được một thread chạm vào. Nếu chuyển sang thread pool thì mỗi session phải có mutex riêng, hoặc dùng lock-free queue.

**7. Sao phải có `buffer_compact` nếu đã có `realloc`?**
Vì `realloc` là thao tác đắt (có thể copy toàn bộ). Nếu dữ liệu cũ đã parse xong thì chỉ cần `memmove` phần còn lại về đầu — không cấp phát, không đổi capacity. Không có bước này thì buffer phình vô hạn dù dữ liệu thực tế rất ít.

**8. Vì sao `consume` phải clamp khi `len > readable`?**
`read_pos` và `write_pos` là `size_t` (không dấu). Nếu `read_pos > write_pos` thì `write_pos - read_pos` **underflow** thành số khổng lồ (~1.8×10¹⁹) → code tưởng có dữ liệu → đọc rác → crash. Clamp là để bảo vệ bất biến `read_pos ≤ write_pos`.

**9. Phần của em có phụ thuộc phần ai không?**
Không. `common/` chỉ dùng thư viện chuẩn POSIX, không `#include` header của server/client. Chiều phụ thuộc là một chiều: server và client phụ thuộc `common/`. Vì vậy em phải xong trước để cả nhóm build được.

**10. Nếu hai máy khác kiến trúc (x86 và ARM) nói chuyện thì sao?**
Vẫn đúng, vì: (a) `#pragma pack(1)` cố định bố cục byte, (b) mọi trường nhiều byte qua `htons/htonl` nên luôn là big-endian trên dây. Đây chính là lý do hai bước đó là bắt buộc chứ không phải "cho đẹp".

---

## PHẦN F — MỘT SLIDE TÓM TẮT (nếu chỉ có 1 phút)

| | |
|---|---|
| **Vấn đề** | TCP là byte stream → không có ranh giới tin nhắn |
| **Ý tưởng** | Length-prefixed framing (như TLS/WebSocket/MQTT) + network byte order (RFC 1700) + bộ đệm tích luỹ |
| **Cách làm** | `protocol.c` định nghĩa frame 12 byte · `buffer.c` ghép mảnh TCP · `network_utils.c` lo socket non-blocking |
| **Vận hành** | Đọc vào buffer → đủ 12 byte thì parse header → đủ `12+topic+payload` thì xử lý → `consume` |
| **Bằng chứng** | 53/53 test pass · 0 warning với `-Werror` · ASan sạch · demo 36 lần `write(1 byte)` vẫn parse đúng |
