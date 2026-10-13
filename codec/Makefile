BACKEND ?= host
DECODER ?= rapid
CFLAGS = -O2 -g -Wall -Wextra -D_FILE_OFFSET_BITS=64
CXXFLAGS = $(CFLAGS) -std=c++11 -pthread
DEPFLAGS = -MMD -MP
OUT = build/$(BACKEND)-$(DECODER)
TARGET = $(OUT)/swqc-gzip
HOST_OBJECTS = $(OUT)/main.o $(OUT)/gzip_stream.o $(OUT)/rapid_reader.o
ZLIB_SOURCES = adler32 crc32 deflate trees zutil
ifeq ($(BACKEND),sunway)
CC = mpicc
CXX = mpicxx
HOST_FLAGS = -mhost -DSWQC_CODEC_SUNWAY=1
CPE_FLAGS = -mslave -msimd -fPIC -DZ_PREFIX -Ivendor/zlib
OBJECTS = $(HOST_OBJECTS) $(OUT)/backend_mpe.o $(OUT)/backend_cpe.o $(OUT)/codec_job.o $(addprefix $(OUT)/z_,$(addsuffix .o,$(ZLIB_SOURCES)))
LINK_FLAGS = -mhybrid
else ifeq ($(BACKEND),host)
OBJECTS = $(HOST_OBJECTS) $(OUT)/backend_host.o $(OUT)/codec_job.o
else
$(error BACKEND must be host or sunway)
endif
ifeq ($(DECODER),rapid)
OBJECTS += $(OUT)/rapid_firstpass.o
CFLAGS += -DSWQC_CODEC_RAPID=1
else ifneq ($(DECODER),zlib)
$(error DECODER must be zlib or rapid)
endif
all: $(TARGET)
STREAM_OBJECTS = $(filter-out $(OUT)/main.o,$(OBJECTS)) $(OUT)/test_stream.o
$(OUT)/test-stream: $(STREAM_OBJECTS)
	$(CXX) $(LINK_FLAGS) $^ -lz -pthread -o $@
$(TARGET): $(OBJECTS)
	$(CXX) $(LINK_FLAGS) $^ -lz -pthread -o $@
$(OUT):
	mkdir -p $@
$(OUT)/%.o: %.cpp codec_job.h | $(OUT)
	$(CXX) $(HOST_FLAGS) $(CXXFLAGS) $(DEPFLAGS) -c $< -o $@
$(OUT)/backend_mpe.o: backend_mpe.c codec_job.h | $(OUT)
	$(CC) $(HOST_FLAGS) $(CFLAGS) $(DEPFLAGS) -c $< -o $@
$(OUT)/backend_cpe.o: backend_cpe.c codec_job.h | $(OUT)
	$(CC) $(CPE_FLAGS) $(CFLAGS) $(DEPFLAGS) -c $< -o $@
$(OUT)/backend_host.o: backend_host.c codec_job.h | $(OUT)
	$(CC) $(CFLAGS) $(DEPFLAGS) -c $< -o $@
$(OUT)/codec_job.o: codec_job.c codec_job.h | $(OUT)
	$(CC) $(CPE_FLAGS) $(CFLAGS) $(DEPFLAGS) -c $< -o $@
$(OUT)/rapid_firstpass.o: rapid_firstpass.cpp codec_job.h | $(OUT)
	$(CXX) $(CPE_FLAGS) $(CFLAGS) $(DEPFLAGS) -DNDEBUG -std=c++17 -Ivendor/rapidgzip -c $< -o $@
$(OUT)/z_%.o: vendor/zlib/%.c | $(OUT)
	$(CC) $(CPE_FLAGS) $(CFLAGS) $(DEPFLAGS) -c $< -o $@
-include $(OBJECTS:.o=.d) $(STREAM_OBJECTS:.o=.d)
.PHONY: all check clean
check: $(TARGET)
ifeq ($(BACKEND),host)
	python3 test_codec.py $(TARGET) $(if $(filter zlib,$(DECODER)),--serial-gzread,)
else
	@echo 'Submit Sunway codec tests to a compute node; do not run check on the login node.' >&2
	@exit 1
endif
clean:
	rm -rf build
