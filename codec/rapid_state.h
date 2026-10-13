#ifndef SWQC_RAPID_STATE_H
#define SWQC_RAPID_STATE_H
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#if defined(SWQC_CODEC_CPE) && SWQC_CODEC_LDM == 2
extern "C" void *ldm_malloc(size_t);
extern "C" void ldm_free(void *, size_t);
#endif

// Fall back to the existing heap if the hot state would crowd the CPE stack.
template<class T>
class SwqcRapidState {
public:
    SwqcRapidState() {
#if defined(SWQC_CODEC_CPE) && SWQC_CODEC_LDM == 2
        backing_ = ldm_malloc(sizeof(T) + 63);
        volatile unsigned char marker;
        uintptr_t start = reinterpret_cast<uintptr_t>(backing_);
        uintptr_t stack = reinterpret_cast<uintptr_t>(&marker);
        if (backing_ && (start >= stack || sizeof(T) + 63 > stack - start ||
            stack - start - sizeof(T) - 63 < (72U << 10))) {
            ldm_free(backing_, sizeof(T) + 63); backing_ = nullptr;
        }
        if (backing_) {
            try { value_ = new (reinterpret_cast<void *>((start + 63) & ~uintptr_t(63))) T; }
            catch (...) { ldm_free(backing_, sizeof(T) + 63); throw; }
        } else
#endif
            heap_.reset(new T);
        if (!value_) value_ = heap_.get();
    }
    ~SwqcRapidState() {
#if defined(SWQC_CODEC_CPE) && SWQC_CODEC_LDM == 2
        if (backing_) { value_->~T(); ldm_free(backing_, sizeof(T) + 63); }
#endif
    }
    SwqcRapidState(const SwqcRapidState &) = delete;
    SwqcRapidState &operator=(const SwqcRapidState &) = delete;
    T *operator->() { return value_; }
    bool in_ldm() const {
#ifdef SWQC_CODEC_CPE
        return reinterpret_cast<uintptr_t>(value_) < (256U << 10);
#else
        return false;
#endif
    }
private:
    std::unique_ptr<T> heap_;
    void *backing_ = nullptr;
    T *value_ = nullptr;
};
#endif
