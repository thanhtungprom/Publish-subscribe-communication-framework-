#!/usr/bin/env bash
set -e

PORT=9005
LOG_VALGRIND="evidence/valgrind_server.log"
REPORT_SS="evidence/ss_connection_report.txt"

echo "=== 1. Starting Broker Server under Valgrind on port $PORT ==="
valgrind --leak-check=full --show-leak-kinds=all --track-fds=yes ./bin/server $PORT > "$LOG_VALGRIND" 2>&1 &
SERVER_PID=$!
echo "Server PID: $SERVER_PID"

sleep 2

echo "=== 2. Recording socket state with ss ==="
echo "--- Socket statistics during server startup ---" > "$REPORT_SS"
ss -tulpn | grep $PORT >> "$REPORT_SS" || true
echo "" >> "$REPORT_SS"

echo "=== 3. Executing Slow Subscriber Flow Control Test ==="
python3 tests/test_slow_subscriber.py 127.0.0.1 $PORT 300

echo "=== 4. Executing Benchmark Load Test ==="
python3 tests/test_benchmark.py 127.0.0.1 $PORT 2 3 100

echo "=== 5. Executing Malformed Input & Fault Tolerance Test ==="
python3 tests/test_malformed.py 127.0.0.1 $PORT

echo "=== 6. Recording socket state after all clients finished ==="
echo "--- Socket statistics after test completion ---" >> "$REPORT_SS"
ss -tulpn | grep $PORT >> "$REPORT_SS" || true
echo "" >> "$REPORT_SS"

echo "=== 7. Sending SIGINT for graceful shutdown ==="
kill -INT $SERVER_PID

# Wait for Valgrind to finish dumping report
wait $SERVER_PID || true

echo "=== 8. Valgrind summary ==="
tail -n 25 "$LOG_VALGRIND"

echo "=== EVIDENCE GENERATION COMPLETED SUCCESSFULLY ==="
