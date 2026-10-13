#ifndef SWQC_DECODED_VIEW_H
#define SWQC_DECODED_VIEW_H
#include "line_index.h"
#include <memory>

// Byte ownership may outlive the reader and its next decode batch.
struct SwqcDecodedView {
    std::shared_ptr<unsigned char> owner;
    unsigned char *data = nullptr;
    size_t size = 0;
    SwqcLineIndex lines;
};
#endif
