#include "Net/Buffer.h"
#include <cassert>
#include <algorithm>
#include <stdexcept>

Buffer::Buffer() : read_ptr_(0), write_ptr_(0) 
{
    setSize(4096);   // 初始容量4KB
}
Buffer::~Buffer() = default;

void Buffer::bufferAppend(const char* msg, size_t len) 
{
    if (len == 0) return;
    if (!msg) throw std::invalid_argument("Buffer null input");
    enableWrite(len);                              // 确保有足够空间
    memcpy(buffer_.data() + write_ptr_, msg, len); // 拷贝数据
    write_ptr_ += len;                             // 移动写指针
}

void Buffer::setSize(size_t size) {
    if (size < write_ptr_) throw std::length_error("Buffer resize truncates readable data");
    buffer_.resize(size);
}
size_t Buffer::getreadptr() const { return read_ptr_; }
size_t Buffer::getwriteptr() const { return write_ptr_; }
const char* Buffer::peek() { return buffer_.data() + read_ptr_; }

void Buffer::enableWrite(size_t len) 
{
    size_t writeable = buffer_.size() - write_ptr_;    // 剩余可写空间
    size_t readable = write_ptr_ - read_ptr_;          // 未消费的数据
    if (writeable < len) 
    {
        // 如果前部有空间（read_ptr_ > 0），将数据移动到头部
        if (writeable + read_ptr_ >= len) 
        {
            memmove(buffer_.data(), buffer_.data() + read_ptr_, readable);
            read_ptr_ = 0;
            write_ptr_ = readable;
        } 
        else 
        {
            // 扩容，按需扩容字节
            if (len > buffer_.max_size() - write_ptr_)
                throw std::length_error("Buffer capacity overflow");
            const size_t required = write_ptr_ + len;
            const size_t growth = buffer_.size() / 2 + 1;
            const size_t grown = buffer_.size() > buffer_.max_size() - growth
                ? buffer_.max_size() : buffer_.size() + growth;
            setSize(std::max(required, grown));
        }
    }
}

size_t Buffer::getreadable() const { return write_ptr_ - read_ptr_; }
void Buffer::moveReadPtr(size_t len) 
{ 
    if (len > write_ptr_ - read_ptr_) throw std::out_of_range("Buffer read offset");
    read_ptr_ += len;

    // 关键：读空时归零，让下次写入从头复用 buffer_ 空间
    // 否则 read_ptr_ 和 write_ptr_ 会一直往后推，buffer_ 只会涨不会缩
    if (read_ptr_ == write_ptr_) 
    {
        read_ptr_  = 0;
        write_ptr_ = 0;
    }
}
void Buffer::moveWritePtr(size_t len) 
{
    if (len > buffer_.size() - write_ptr_) throw std::out_of_range("Buffer write offset"); // 确保不超过剩余可写空间
    write_ptr_ += len; 
}