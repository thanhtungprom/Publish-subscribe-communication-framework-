CC ?= gcc
CFLAGS ?= -std=gnu11 -Wall -Wextra -Wpedantic -O2 -Iinclude -D_GNU_SOURCE
LDFLAGS ?= 

BIN_DIR = bin
BUILD_DIR = build
SRC_DIR = src
TEST_DIR = tests

# Common objects
COMMON_SRCS = $(SRC_DIR)/common/protocol.c \
              $(SRC_DIR)/common/buffer.c \
              $(SRC_DIR)/common/network_utils.c
COMMON_OBJS = $(patsubst $(SRC_DIR)/common/%.c, $(BUILD_DIR)/common/%.o, $(COMMON_SRCS))

# Server objects
SERVER_SRCS = $(SRC_DIR)/server/main.c \
              $(SRC_DIR)/server/server_epoll.c \
              $(SRC_DIR)/server/client_session.c \
              $(SRC_DIR)/server/topic_manager.c
SERVER_OBJS = $(patsubst $(SRC_DIR)/server/%.c, $(BUILD_DIR)/server/%.o, $(SERVER_SRCS))

# Client objects
PUB_SRCS = $(SRC_DIR)/client/publisher.c
PUB_OBJS = $(BUILD_DIR)/client/publisher.o

SUB_SRCS = $(SRC_DIR)/client/subscriber.c
SUB_OBJS = $(BUILD_DIR)/client/subscriber.o

TARGET_SERVER = $(BIN_DIR)/server
TARGET_PUB = $(BIN_DIR)/publisher
TARGET_SUB = $(BIN_DIR)/subscriber
TARGET_TEST_FRAMING = $(BIN_DIR)/test_framing

all: directories $(TARGET_SERVER) $(TARGET_PUB) $(TARGET_SUB) $(TARGET_TEST_FRAMING)

directories:
	@mkdir -p $(BIN_DIR) $(BUILD_DIR)/common $(BUILD_DIR)/server $(BUILD_DIR)/client $(BUILD_DIR)/tests

$(TARGET_SERVER): $(SERVER_OBJS) $(COMMON_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(TARGET_PUB): $(PUB_OBJS) $(COMMON_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(TARGET_SUB): $(SUB_OBJS) $(COMMON_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(TARGET_TEST_FRAMING): $(BUILD_DIR)/tests/test_framing.o $(COMMON_OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(BUILD_DIR)/common/%.o: $(SRC_DIR)/common/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/server/%.o: $(SRC_DIR)/server/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/client/%.o: $(SRC_DIR)/client/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(BUILD_DIR)/tests/test_framing.o: $(TEST_DIR)/test_framing.c
	$(CC) $(CFLAGS) -c $< -o $@

test: $(TARGET_TEST_FRAMING)
	@echo "--- Running Unit Tests ---"
	./$(TARGET_TEST_FRAMING)

valgrind_test: $(TARGET_TEST_FRAMING)
	@echo "--- Checking Memory with Valgrind ---"
	valgrind --leak-check=full --show-leak-kinds=all --track-fds=yes ./$(TARGET_TEST_FRAMING)

clean:
	rm -rf $(BUILD_DIR) $(BIN_DIR)

# [ADDED] Generate compile_commands.json for IDE / clangd LSP header resolution (-Iinclude)
ALL_SRCS = $(COMMON_SRCS) $(SERVER_SRCS) $(PUB_SRCS) $(SUB_SRCS)
compile_commands:
	@echo "[" > compile_commands.json
	@first=1; \
	for src in $(ALL_SRCS); do \
		if [ $$first -eq 0 ]; then echo "," >> compile_commands.json; fi; \
		first=0; \
		printf '  {\n    "directory": "%s",\n    "command": "%s %s -c %s",\n    "file": "%s/%s"\n  }' \
			"$(CURDIR)" "$(CC)" "$(CFLAGS)" "$$src" "$(CURDIR)" "$$src" >> compile_commands.json; \
	done
	@echo "" >> compile_commands.json
	@echo "]" >> compile_commands.json
	@echo "Generated compile_commands.json"

.PHONY: all clean directories test valgrind_test compile_commands
