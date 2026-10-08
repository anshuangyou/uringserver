#pragma once
#include "log.hpp"
#include <atomic>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

//缓冲容量口径
constexpr size_t BUFFER_DEFAULT_CAPACITY=1024;          //默认1KB
constexpr size_t BUFFER_MAX_CAPACITY=10*1024*1024;      //单块缓冲的容量上限:10MB
//对象池只回收"真实分配不超过这个值"的缓冲,被大报文撑大的缓冲直接释放,避免池子长期占大内存
constexpr size_t BUFFER_POOL_MAX_REUSED_CAPACITY=4*1024;
//这三个常量之间有真实耦合,写错就是运行期才炸(例如池子回收的容量超过上限)
static_assert(BUFFER_DEFAULT_CAPACITY<=BUFFER_MAX_CAPACITY,"default capacity must not exceed the limit");
static_assert(BUFFER_POOL_MAX_REUSED_CAPACITY<=BUFFER_MAX_CAPACITY,"pool reuse threshold must not exceed the limit");

class Buffer
{
private:
    size_t                  _read_index{0};     //已消费的字节数
    size_t                  _write_index{0};    //已写入的字节数
    size_t                  _default_capacity;  //缩容时的下限(通常等于构造时给的容量)
    size_t                  _max_capacity;      //容量上限,扩容绝不越过它
    size_t                  _capacity{0};       //当前逻辑容量(shrink之后可以小于_allocated)
    size_t                  _allocated{0};      //真实分配到的字节数(池子判定用)
    //裸分配而不是std::vector:new char[]对char不做初始化,扩容不会白写一遍0(实测vector::resize会把新区域全部清零)
    std::unique_ptr<char[]> _storage;
public:
    explicit Buffer(size_t capacity=BUFFER_DEFAULT_CAPACITY,size_t max_capacity=BUFFER_MAX_CAPACITY)
    :_default_capacity(check_capacity(capacity,max_capacity))
    ,_max_capacity(max_capacity)
    ,_capacity(capacity)
    ,_allocated(capacity)
    ,_storage(new char[capacity])
    {
    }
    Buffer(const Buffer&)=delete;
    Buffer& operator=(const Buffer&)=delete;
private:
    //非法参数(0容量、capacity>max)必须当场拦下:静默接受的话"容量上限绝不越过"这条不变量就废了。
    //用throw而不是assert:assert在NDEBUG下会消失,而且BufferPool::acquire要能把它当普通异常捕获后返回nullptr
    static size_t check_capacity(size_t capacity,size_t max_capacity)
    {
        if(capacity==0||capacity>max_capacity)
        {
            throw std::invalid_argument("Buffer: capacity must be in [1,max_capacity]");
        }
        return capacity;
    }
    //换一块新存储,并把可读数据搬到新块头部(顺带压缩);bad_alloc时原状态不变
    void reallocate(size_t new_capacity)
    {
        std::unique_ptr<char[]> fresh(new char[new_capacity]);
        const size_t readable=readable_size();
        if(readable>0){std::memcpy(fresh.get(),read_ptr(),readable);}
        _storage=std::move(fresh);
        _capacity=new_capacity;
        _allocated=new_capacity;
        _read_index=0;
        _write_index=readable;
    }
public:
    //物理起始地址(只用于需要整块内存的场景,正常读写用read_ptr/write_ptr)
    char* data()noexcept{return _storage.get();}
    const char* data()const noexcept{return _storage.get();}
    //可读区起点:交给send/write的指针
    char* read_ptr()noexcept{return _storage.get()+_read_index;}
    const char* read_ptr()const noexcept{return _storage.get()+_read_index;}
    //可写区起点:交给recv/read的指针
    char* write_ptr()noexcept{return _storage.get()+_write_index;}
    const char* write_ptr()const noexcept{return _storage.get()+_write_index;}
    //可读字节数
    size_t readable_size()const noexcept{return _write_index-_read_index;}
    //尾部还能直接写多少字节(不含前移之后能腾出来的空间)
    size_t writable_size()const noexcept{return _capacity-_write_index;}
    //头部已经消费掉,可以被前移复用的空间
    size_t leading_space()const noexcept{return _read_index;}
    //当前逻辑容量(也是缩容的下限口径)
    size_t capacity()const noexcept{return _capacity;}
    //真实分配到的字节数:shrink()之后它会大于capacity(),池子判定"这块大到不该回收"时必须用它
    size_t allocated_capacity()const noexcept{return _allocated;}
    size_t max_capacity()const noexcept{return _max_capacity;}
    bool empty()const noexcept{return _read_index==_write_index;}
    //整块可读/可写区域一次拿到
    std::span<const char> readable()const noexcept{return {read_ptr(),readable_size()};}
    std::span<char> readable()noexcept{return {read_ptr(),readable_size()};}
    std::span<char> writable()noexcept{return {write_ptr(),writable_size()};}
    //整块存储(只读,用于需要看全量的场景)
    std::span<const char> storage()const noexcept{return {_storage.get(),_capacity};}
    //----------------容量管理----------------
    //容量至少扩到new_capacity;已经分配过这么大时只放大逻辑容量,不重新分配也不搬数据
    void reserve(size_t new_capacity)
    {
        if(new_capacity>_max_capacity){new_capacity=_max_capacity;}
        if(new_capacity<=_capacity){return;}
        if(new_capacity<=_allocated){_capacity=new_capacity;return;}
        reallocate(new_capacity);
    }
    //保证至少有len字节剩余空间
    void reserve_for(size_t len)
    {
        if(len==0){return;}
        if(len>_max_capacity)
        {
            throw std::length_error("Buffer::reserve_for requested length exceeds the capacity limit");
        }
        if(writable_size()>=len){return;}
        //尾部不够,尝试通过前移已消费的头部空间来腾出空间
        const size_t readable=readable_size();
        if(_read_index>0)
        {
            //统一前移,让_read_index归零_write_index指向实际数据末尾
            if(readable>0){std::memmove(_storage.get(),read_ptr(),readable);}
            _read_index=0;
            _write_index=readable;
            //前移后空间够了,直接返回
            if(writable_size()>=len){return;}
        }
        //前移后空间仍然不够,计算扩容后的容量。
        //用减法判溢出:len<=_max_capacity已经保证,但_write_index+len在_max_capacity极大时仍会回绕,
        //回绕后"required>_max_capacity"不成立就会静默返回,调用方按承诺往write_ptr()写len字节直接越界
        if(len>_max_capacity-_write_index)
        {
            throw std::length_error("Buffer::reserve_for required size exceeds the capacity limit");
        }
        const size_t required=_write_index+len;
        //优先翻倍,但不超过上限,且必须满足required
        reallocate(std::min(std::max(_capacity*2,required),_max_capacity));
    }
    //收缩空间(只改逻辑长度,不把内存还给分配器)
    //故意不做真正的realloc:recv路径是"每次回调只能填writable_size()、一个满缓冲要翻好几次倍才长得回来",
    //在"小包/大包严格交替"的流量下每次回调都realloc会带来60ns级的抖动。
    //要真的把内存还给分配器,请在连接空闲/关闭这种非热路径上调trim()
    void shrink()
    {
        const size_t readable=readable_size();
        //前移
        if(_read_index>0)
        {
            if(readable>0){std::memmove(_storage.get(),read_ptr(),readable);}
            _read_index=0;
            _write_index=readable;
        }
        //容量大于默认值且使用率低于一半就缩容
        if(_capacity>_default_capacity&&readable<_capacity/2)
        {
            _capacity=std::max(_default_capacity,readable*2);
        }
    }
    //硬缩容:连分配出来的内存一起还给分配器(会realloc)。只适合在连接空闲/关闭这类非热路径调用
    void trim()noexcept
    {
        shrink();
        if(_allocated>_capacity)
        {
            try{
                reallocate(_capacity);
            }catch(...){
                //还内存失败不影响正确性:数据仍在,只是没还回去
            }
        }
    }
    //丢弃可读数据但保留容量
    void reset()noexcept
    {
        _read_index=0;
        _write_index=0;
    }
    //----------------追加----------------
    void append(const void* src,size_t len)
    {
        if(len==0){return;}
        if(src==nullptr)[[unlikely]]
        {
            ERROR("buffer append ignored: source is null but len={}",len);
            return;
        }
        const char* p=static_cast<const char*>(src);
        //src可能指向本缓冲自己(append(readable())、append(read_ptr(),n)):reserve_for会前移甚至重新分配存储,
        //那样memcpy读到的就是已经搬走/已经释放的内存(ASAN实测heap-use-after-free)。这里先把源字节拷出来
        const char* base=_storage.get();
        const bool self=!std::less<const char*>()(p,base)&&std::less<const char*>()(p,base+_capacity);
        if(self)
        {
            const std::string tmp(p,len);
            reserve_for(len);
            std::memcpy(write_ptr(),tmp.data(),len);
            (void)commit(len);
            return;
        }
        reserve_for(len);
        std::memcpy(write_ptr(),src,len);
        (void)commit(len);
    }
    void append(const std::string& text){append(text.data(),text.size());}
    void append(std::string_view text){append(text.data(),text.size());}
    void append(std::span<const char> src){append(src.data(),src.size());}
    //按C字符串追加(长度取strlen)。有这个精确重载,append("字面量")才不会被string_view/span两个用户定义转换搞成歧义
    void append(const char* text)
    {
        if(text==nullptr)[[unlikely]]
        {
            ERROR("buffer append ignored: source is null");
            return;
        }
        append(text,std::strlen(text));
    }
    //----------------读取:不消费----------------
    //把可读数据拷进dst,返回实际拷贝的字节数(不动读游标)
    [[nodiscard]] size_t peek(void* dst,size_t len)const
    {
        if(len==0||dst==nullptr){return 0;}
        const size_t n=std::min(len,readable_size());
        if(n==0){return 0;}
        //dst可能指向本缓冲内部(例如把已读数据挪到可写区):memcpy遇到重叠区间是UB,用memmove
        std::memmove(dst,read_ptr(),n);
        return n;
    }
    [[nodiscard]] std::string peek_string(size_t len)const
    {
        const size_t n=std::min(len,readable_size());
        if(n==0){return std::string();}
        return std::string(read_ptr(),n);
    }
    //零拷贝视图:只在"这块缓冲被改动之前"有效
    //(任何append/commit/consume/reserve/reserve_for/shrink/trim/reset之后都会作废)
    [[nodiscard]] std::string_view peek_view()const noexcept{return {read_ptr(),readable_size()};}
    //取出一整行(含换行符),返回std::nullopt表示还没有收到完整的行(和空行区分开)
    [[nodiscard]] std::optional<std::string> peek_line()const
    {
        const char* newline=find_newline();
        if(newline==nullptr){return std::nullopt;}
        return peek_string(static_cast<size_t>(newline-read_ptr())+1);
    }
    [[nodiscard]] std::optional<std::string_view> peek_line_view()const noexcept
    {
        const char* newline=find_newline();
        if(newline==nullptr){return std::nullopt;}
        return std::string_view(read_ptr(),static_cast<size_t>(newline-read_ptr())+1);
    }
    [[nodiscard]] const char* find_newline()const noexcept
    {
        const size_t n=readable_size();
        if(n==0){return nullptr;}//空缓冲提前返回,不麻烦memchr
        return static_cast<const char*>(std::memchr(read_ptr(),'\n',n));
    }
    [[nodiscard]] char* find_newline()noexcept
    {
        const size_t n=readable_size();
        if(n==0){return nullptr;}
        return static_cast<char*>(std::memchr(read_ptr(),'\n',n));
    }
    //----------------读取:消费----------------
    size_t take(void* dst,size_t len)
    {
        const size_t n=peek(dst,len);
        (void)consume(n);//n来自peek,一定不超过readable_size()
        return n;
    }
    [[nodiscard]] std::string take_string(size_t len)
    {
        const size_t n=std::min(len,readable_size());
        if(n==0){return std::string();}
        std::string out(read_ptr(),n);
        (void)consume(n);
        return out;
    }
    [[nodiscard]] std::optional<std::string> take_line()
    {
        const char* newline_=find_newline();
        if(newline_==nullptr){return std::nullopt;}
        const size_t n=static_cast<size_t>(newline_-read_ptr())+1;
        std::string line(read_ptr(),n);
        (void)consume(n);
        return line;
    }
    //----------------游标推进----------------
    //消费掉已经处理完的len字节(只能由拥有这块缓冲的io线程调用);越界返回false且游标不动
    [[nodiscard]] bool consume(size_t len)noexcept
    {
        if(len==0){return true;}
        if(len>readable_size())[[unlikely]]
        {
            ERROR("buffer consume ignored: requested={} bytes but only {} are readable",len,readable_size());
            return false;
        }
        _read_index+=len;
        //全部读走就复位,让下一次写入直接从头部开始
        if(_read_index==_write_index){reset();}
        return true;
    }
    //把已经直接写进可写区的len字节登记为可读(内核recv完成后用);越界返回false且游标不动
    [[nodiscard]] bool commit(size_t len)noexcept
    {
        if(len==0){return true;}
        if(len>writable_size())[[unlikely]]
        {
            ERROR("buffer commit ignored: requested={} bytes but only {} are writable",len,writable_size());
            return false;
        }
        _write_index+=len;
        return true;
    }
};


//Buffer对象池
//注意:BufferPool必须由shared_ptr<BufferPool>持有,否则无法自动回收Buffer
class BufferPool:public std::enable_shared_from_this<BufferPool>
{
private:
    mutable std::mutex                              _mtx;
    //按max_capacity分桶:一个池子常常同时服务"接收缓冲(max=64KB)"与"发送缓冲(max=10MB)",
    //混在一个栈里的话弹出的那块几乎总是max对不上,acquire就退化成delete+new,池子等于白搭
    std::unordered_map<size_t,std::vector<Buffer*>> _pool;
    size_t                                          _free_total{0};                             //池内Buffer总数(所有桶加起来)
    size_t                                          _max;
    std::atomic_uint64_t                            _acquire_count{0};                          //累计借出次数(观测用:判断池子是不是真的在被复用)
    std::atomic_uint64_t                            _alloc_count{0};                            //累计不得不新建Buffer的次数
public:
    explicit BufferPool(size_t max_buffers=1024)
    :_max(max_buffers)
    {
    }
    ~BufferPool()
    {
        std::lock_guard<std::mutex> lock(_mtx);
        for(auto& bucket:_pool){for(Buffer* buf:bucket.second){delete buf;}}
        _pool.clear();
        _free_total=0;
    }
    BufferPool(const BufferPool&)=delete;
    BufferPool& operator=(const BufferPool&)=delete;
public:
    size_t free_count()const
    {
        std::lock_guard<std::mutex> lock(_mtx);
        return _free_total;
    }
    uint64_t acquire_count()const{return _acquire_count.load(std::memory_order_relaxed);}
    uint64_t alloc_count()const{return _alloc_count.load(std::memory_order_relaxed);}
public:
    //获取一个Buffer(返回的shared_ptr<Buffer>析构时自动归还Buffer)
    //size==0或size>max_size是调用方错误:统一返回nullptr,不要出现"池里有货就给一块正常缓冲、池空就抛"的不一致行为
    std::shared_ptr<Buffer> acquire(size_t size=BUFFER_DEFAULT_CAPACITY,size_t max_size=BUFFER_MAX_CAPACITY)
    {
        if(size==0||size>max_size)[[unlikely]]
        {
            ERROR("cannot acquire a buffer: buffer_size={} is invalid for max_size={}",size,max_size);
            return nullptr;
        }
        Buffer* raw=nullptr;
        {
            std::lock_guard<std::mutex> lock(_mtx);
            auto it=_pool.find(max_size);
            if(it!=_pool.end())
            {
                std::vector<Buffer*>& bucket=it->second;
                //容量够装下size就能复用(不必严格相等):缓冲一旦被大报文撑大,严格相等会让它永远回不到调用方手里。
                //从尾部往前找:稳态下最后放回去的那块就能用,所以通常一次就命中
                for(size_t i=bucket.size();i>0;i--)
                {
                    if(bucket[i-1]->capacity()>=size)
                    {
                        raw=bucket[i-1];
                        bucket.erase(bucket.begin()+static_cast<std::ptrdiff_t>(i-1));
                        _free_total--;
                        break;
                    }
                }
                if(bucket.empty()){_pool.erase(it);}
            }
        }
        if(!raw)
        {
            _alloc_count.fetch_add(1,std::memory_order_relaxed);
            try{
                raw=new Buffer(size,max_size);
            }catch(const std::exception& e){
                ERROR("cannot allocate a buffer from the pool: buffer_size={}, max_size={}, error={}",size,max_size,e.what());
                return nullptr;
            }
        }
        raw->reset();
        _acquire_count.fetch_add(1,std::memory_order_relaxed);
        //为Buffer的shared_ptr自定义Deleter,在调用delete时可自动归还Buffer
        std::weak_ptr<BufferPool> weak=weak_from_this();
        return std::shared_ptr<Buffer>(raw,[weak](Buffer* buf)
        {
            auto pool=weak.lock();
            if(pool){pool->release(buf);}
            else{delete buf;}
        });
    }
private:
    //归还一个Buffer(acquire返回的shared_ptr<Buffer>析构时自动调用;不要手动调,否则同一块缓冲会被归还两次)
    void release(Buffer* buf)
    {
        if(!buf){return;}
        //被大报文撑大的缓冲直接销毁,不回池。
        //注意要比"真实分配"而不是逻辑容量:shrink()只改逻辑长度,按逻辑容量判会把一块大内存当小缓冲收进池子
        if(buf->allocated_capacity()>BUFFER_POOL_MAX_REUSED_CAPACITY)
        {
            delete buf;
            return;
        }
        //重置并放回
        buf->reset();
        std::lock_guard<std::mutex> lock(_mtx);
        if(_free_total>=_max)
        {
            delete buf;
            return;
        }
        _pool[buf->max_capacity()].push_back(buf);
        _free_total++;
    }
};
