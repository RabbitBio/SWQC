EXEC_MODE ?= single
CPE_ALLOCATOR ?= system
CPE_LDM_POOL ?= 0
CPE_MANUAL_LDM ?= 1
LDM_POOL_BYTES ?= 32768
GZIP_DECODER ?= rapid
GZIP_CPE_LDM ?= 2
ifneq ($(filter $(GZIP_CPE_LDM),0 1 2),$(GZIP_CPE_LDM))
$(error GZIP_CPE_LDM must be 0 (heap), 1 (existing pool), or 2 (hot state in LDM))
endif
ifeq ($(GZIP_CPE_LDM),1)
ifneq ($(CPE_LDM_POOL),1)
$(error GZIP_CPE_LDM=1 requires CPE_LDM_POOL=1)
endif
endif
ifneq ($(filter $(CPE_MANUAL_LDM),0 1),$(CPE_MANUAL_LDM))
$(error CPE_MANUAL_LDM must be 0 or 1)
endif
ifneq ($(filter $(CPE_LDM_POOL),0 1),$(CPE_LDM_POOL))
$(error CPE_LDM_POOL must be 0 or 1)
endif
ifeq ($(EXEC_MODE),single)
MODE_ID := SWQC_SINGLE
else ifeq ($(EXEC_MODE),cgs)
MODE_ID := SWQC_CGS
else ifeq ($(EXEC_MODE),cgs_cross)
MODE_ID := SWQC_CROSS
else
$(error EXEC_MODE must be single, cgs or cgs_cross)
endif
ifeq ($(CPE_ALLOCATOR),system)
POOL_ID := 0
else ifeq ($(CPE_ALLOCATOR),pool)
POOL_ID := 1
else
$(error CPE_ALLOCATOR must be system or pool)
endif
CONFIG_FLAGS := -include common/swqc_config.h -DSWQC_EXEC_MODE=$(MODE_ID) -DSWQC_POOL=$(POOL_ID)
CONFIG_FLAGS += -DSWQC_LDM_POOL=$(CPE_LDM_POOL) -DSWQC_LDM_POOL_BYTES=$(LDM_POOL_BYTES)
CONFIG_FLAGS += -DSWQC_MANUAL_LDM=$(CPE_MANUAL_LDM)
CONFIG_FLAGS += -DSWQC_CODEC_LDM=$(GZIP_CPE_LDM)
DEPFLAGS := -MMD -MP

DIR_SRC := ./src
SLAVE_DIR_SRC := ./slave
DIR_OBJ := ./obj


PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
LIBRARY_DIRS ?=

SRC := $(wildcard ${DIR_SRC}/*.cpp)
OBJ := $(patsubst %.cpp,${DIR_OBJ}/%.o,$(notdir ${SRC}))


SRC2 := $(wildcard ${DIR_SRC}/*.c)
OBJ += $(patsubst %.c,${DIR_OBJ}/%.o,$(notdir ${SRC2}))


SRC3 := $(wildcard ${SLAVE_DIR_SRC}/*.cpp)
OBJ += $(patsubst %.cpp,${DIR_OBJ}/%.o,$(notdir ${SRC3}))


SRC4 := $(wildcard ${SLAVE_DIR_SRC}/lib/*.c)
OBJ += $(patsubst %.c,${DIR_OBJ}/slave_%.o,$(notdir ${SRC4}))

CODEC_ZLIB := adler32 crc32 deflate trees zutil
OBJ += obj/gzip_stream.o obj/rapid_reader.o obj/codec_job.o obj/codec_backend_cpe.o
OBJ += $(addprefix obj/codec_z_,$(addsuffix .o,$(CODEC_ZLIB)))
CODEC_FLAGS := -DZ_PREFIX -Icodec/vendor/zlib -DSWQC_CODEC_CPE=1
ifeq ($(GZIP_DECODER),rapid)
OBJ += obj/codec_rapid_firstpass.o
CODEC_FLAGS += -DSWQC_CODEC_RAPID=1
CODEC_HOST_FLAGS := -DSWQC_CODEC_RAPID=1
else ifneq ($(GZIP_DECODER),zlib)
$(error GZIP_DECODER must be zlib or rapid)
endif



TARGET := SWQC 

BIN_TARGET := ${TARGET}


CXX = mpicxx


CXXFLAGS := $(InstructSet)
CXXFLAGS += -DVerbose -std=c++11 -I./ -I./common -g -O3 -w $(CONFIG_FLAGS)


CXX2 = mpicc

CXXFLAGS2 := -std=gnu11 -I./common -g -O3 -w $(CONFIG_FLAGS)

LIBS := -static -lz -lpthread -lrt -Wl,-q


LD_FLAGS := $(foreach librarydir,$(LIBRARY_DIRS),-L$(librarydir)) $(LIBS)

all: ${BIN_TARGET}

${BIN_TARGET}:${OBJ}
		@set -e; command="$$($(CXX) -show -mhybrid $^ -o $@ $(LD_FLAGS) | sed 's@/single_static@/multi_static@g')"; \
		echo "$$command"; eval "$$command"

${DIR_OBJ}/%.o:${DIR_SRC}/%.cpp
		$(CXX) -mhost -c $< -o $@ $(CXXFLAGS) $(DEPFLAGS)
${DIR_OBJ}/%.o:${SLAVE_DIR_SRC}/%.cpp
		$(CXX) -mslave -msimd -fPIC -c $< -o $@ $(CXXFLAGS) $(DEPFLAGS)
${DIR_OBJ}/%.o:${DIR_SRC}/%.c
		$(CXX2) -mhost -c $< -o $@ $(CXXFLAGS2) $(DEPFLAGS)
$(DIR_OBJ)/slave_%.o: ${SLAVE_DIR_SRC}/lib/%.c
		$(CXX2) -mslave -msimd -fPIC -c $< -o $@ $(CXXFLAGS2) $(DEPFLAGS)

obj/gzip_stream.o: codec/gzip_stream.cpp
	$(CXX) -mhost -c $< -o $@ $(CXXFLAGS) $(DEPFLAGS) $(CODEC_HOST_FLAGS) -D_FILE_OFFSET_BITS=64
obj/rapid_reader.o: codec/rapid_reader.cpp
	$(CXX) -mhost -c $< -o $@ $(CXXFLAGS) $(DEPFLAGS) -D_FILE_OFFSET_BITS=64
obj/codec_job.o: codec/codec_job.c
	$(CXX2) -mslave -msimd -fPIC -c $< -o $@ $(CXXFLAGS2) $(DEPFLAGS) $(CODEC_FLAGS)
obj/codec_backend_cpe.o: codec/backend_cpe.c
	$(CXX2) -mslave -msimd -fPIC -c $< -o $@ $(CXXFLAGS2) $(DEPFLAGS)
obj/codec_z_%.o: codec/vendor/zlib/%.c
	$(CXX2) -mslave -msimd -fPIC -c $< -o $@ $(CXXFLAGS2) $(DEPFLAGS) $(CODEC_FLAGS)
obj/codec_rapid_firstpass.o: codec/rapid_firstpass.cpp
	$(CXX) -mslave -msimd -fPIC -c $< -o $@ $(CXXFLAGS) $(DEPFLAGS) -DNDEBUG -std=c++17 $(CODEC_FLAGS) -Icodec/vendor/rapidgzip





-include $(OBJ:.o=.d)

.PHONY: all clean install
clean:
	rm -f $(DIR_OBJ)/*.o $(DIR_OBJ)/*.d
	rm -rf $(TARGET)
	rm -f SWQC.debug SWQC.compact

install:
	install $(TARGET) $(BINDIR)/$(TARGET)
	@echo "Installed."
