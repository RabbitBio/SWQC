#include "io_window.h"
#include <cassert>
#include <string>
#include <vector>

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    char path[] = "/tmp/swqc-write-window-XXXXXX";
    int fd = mkstemp(path);
    assert(fd >= 0);
    close(fd);
    for (bool posix : {false, true}) {
    for (size_t capacity : {size_t(0), size_t(7), size_t(4096)}) {
        MPI_File file;
        assert(MPI_File_open(MPI_COMM_SELF, path, MPI_MODE_RDWR, MPI_INFO_NULL, &file) == MPI_SUCCESS);
        assert(MPI_File_set_size(file, 0) == MPI_SUCCESS);
        if (posix) MPI_File_close(&file);
        std::unique_ptr<SwqcWriteWindow> holder(posix ? new SwqcWriteWindow(path, capacity)
                                                    : new SwqcWriteWindow(file, capacity));
        SwqcWriteWindow &writer = *holder;
        std::string expected;
        for (int i = 0; i < 500; ++i) {
            std::string piece((i * 37) % 103, 'A' + i % 26);
            writer.append(piece.data(), piece.size(), expected.size());
            expected += piece;
        }
        // Flush at an offset discontinuity; a later piece fills the gap.
        writer.append("tail", 4, expected.size() + 11);
        writer.append("01234567890", 11, expected.size());
        writer.close();
        expected += "01234567890tail";
        assert(writer.bytes == expected.size());
        if (posix) assert(MPI_File_open(MPI_COMM_SELF, path, MPI_MODE_RDONLY, MPI_INFO_NULL, &file) == MPI_SUCCESS);
        MPI_Offset size;
        assert(MPI_File_get_size(file, &size) == MPI_SUCCESS && size == (MPI_Offset)expected.size());
        std::vector<char> actual(expected.size());
        assert(MPI_File_read_at(file, 0, actual.data(), actual.size(), MPI_BYTE, MPI_STATUS_IGNORE) == MPI_SUCCESS);
        assert(std::string(actual.begin(), actual.end()) == expected);
        MPI_File_close(&file);
    }
    }
    unlink(path);
    MPI_Finalize();
    puts("PASS: output windows, partial flushes, empty pieces and discontinuous offsets");
}
