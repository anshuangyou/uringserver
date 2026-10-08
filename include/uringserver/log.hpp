#pragma once
#include <atomic>
#include <condition_variable>
#include <thread>
#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <format>
#include <functional>
#include <iterator>
#include <mutex>
#include <optional>
#include <source_location>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

//fork语义(用之前必读):本文件注册了pthread_atfork的child handler,子进程里日志器会退化成"每条日志当场同步写",
//从而不可能再出现"子进程等一个不存在的后台线程"的永久挂死(见构造函数与fork_child处的实现注释)。以下残余限制是有意接受的:
//1.fork那一刻还没交给后台线程的_front/_out内容会丢(只影响子进程,父进程不受影响),不做抢救;
//2.子进程继承父进程的_fd/_file_index/_current_line,会继续往同一个日志文件追加;多进程同时写同一文件时行可能交错,需要严格隔离就在fork后先flush()再改日志目录或文件名;
//3.[tN]是进程内短编号,子进程从t0重新开始,跨进程看会重号;
//4.handler里对两把锁做placement-new、对_out做移动:这些严格说不是async-signal-safe,但在"fork返回后立刻在子进程里用日志"这一场景下是本库明确接受的设计假设;
//5.只保证fork返回后子进程的log()/flush()可用;fork恰在write_batch中途、formatter重入时、日志轮转/写盘出错时这几种时序,以及非主线程fork,都未做用例验证(handler只碰纯内存与锁,理论上应可行);vfork/posix_spawn不走atfork handler,不在覆盖范围内;
//6.子进程调exit()时atexit里的flush走同步分支是安全的;但fork那一刻父进程其它线程已入队、尚未落盘的日志在子进程里不可见。

//编译期常量,可用-D指定
#ifndef LOG_MAX_LINES
inline constexpr uint32_t LOG_MAX_LINES=10240;          //单个日志文件最多多少行,超过就换下一个文件
#endif
#ifndef LOG_BUF_SIZE
inline constexpr uint32_t LOG_BUF_SIZE=4096;            //单条日志(含前缀)的最大长度,超出会被截断
#endif
#ifndef LOG_MIN_LEVEL
inline constexpr int LOG_MIN_LEVEL=0;                   //编译期最小等级:定义为2可编译掉TRACE/DEBUG
#endif
#ifndef LOG_RECORDS_PER_BUF
inline constexpr uint32_t LOG_RECORDS_PER_BUF=512;      //单块双缓冲最多装多少条Record,满了就交给后台
#endif
#ifndef LOG_FLUSH_INTERVAL_MS
inline constexpr uint32_t LOG_FLUSH_INTERVAL_MS=200;    //定时刷盘时间间隔
#endif
#ifndef LOG_WRITE_BLOCK
inline constexpr size_t LOG_WRITE_BLOCK=64*1024;        //后台把这么多字节日志拼成一块再一次write,避免每条日志一次写系统调用
#endif
#ifndef LOG_DEFAULT_PATH
inline constexpr std::string_view LOG_DEFAULT_PATH="./log/";  //编译期默认日志目录:Log::init()没给path时用它
#endif

static_assert(LOG_RECORDS_PER_BUF>0,"LOG_RECORDS_PER_BUF must be positive");
static_assert(LOG_BUF_SIZE>=64,"LOG_BUF_SIZE is too small");
static_assert(LOG_WRITE_BLOCK>0,"LOG_WRITE_BLOCK must be positive");

//单调时钟纳秒值(打开失败后的退避用)
inline int64_t log_now_ns(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}

//把errno转成可读文本(负数取反)
inline std::string errno_text(int err){return std::error_code(err<0?-err:err,std::generic_category()).message();}

//双缓冲+后台线程:业务线程只做等级过滤/取时间戳/把参数按值搬进Record,格式化+落盘在后台线程进行
//配置入口:启动时(第一条日志之前、起线程之前)调用 Log::init(...) 一次性设好路径与等级等;不调也能用,路径取编译期LOG_DEFAULT_PATH
//路径只在init时定死一次,运行期改路径不支持(用set_level/set_max_lines/set_flush_interval_ms这些改行为)
//限制:不是异步信号安全的(不要在信号处理函数里打日志);fork之后子进程里没有后台线程,日志自动退化成同步写(父进程fork那一刻还没落盘的记录会丢);进程退出过程中(静态析构顺序)打日志会被直接丢弃(log()/flush()立刻返回),此时不保证ERROR/FATAL已落盘
//[时间][等级][线程编号][文件名:行号]: 正文
class Log
{
public:
    enum class Level:int
    {
        TRACE=0,
        DEBUG=1,
        INFO=2,
        WARN=3,
        ERROR=4,
        FATAL=5,
        OFF=6,    //比FATAL更高:设成它等于完全关闭日志
    };
    static constexpr std::array<std::string_view,7> level_string=
    {
        "TRACE",
        "DEBUG",
        "INFO",
        "WARN",
        "ERROR",
        "FATAL",
        "OFF"
    };
    static constexpr std::array<std::string_view,7> level_console_color=
    {
        "\033[90m",    //TRACE灰
        "\033[36m",    //DEBUG青
        "\033[32m",    //INFO绿
        "\033[33m",    //WARN黄
        "\033[31m",    //ERROR红
        "\033[1;31m",  //FATAL加粗红
        ""             //OFF无
    };
    //启动期配置:用指定初始化器只写关心的项,以后加字段不会破坏已有调用点。
    //注意:指定初始化器必须按成员声明顺序写(如 {.path=...,.level=...} 合法,{.level=...,.path=...} 不合法)
    struct Config
    {
        std::filesystem::path path{};                       //空=用编译期LOG_DEFAULT_PATH;想显式要当前目录请传"."
        Level                 level{Level::INFO};           //控制台与文件同时生效
        std::optional<Level>  console_level{};              //非空则覆盖level(只影响控制台)
        std::optional<Level>  file_level{};                 //非空则覆盖level(只影响文件)
        bool                  console{true};
        std::optional<bool>   color{};                      //空=stdout是终端就上色,重定向到文件就关闭
        bool                  show_os_tid{false};
        uint32_t              max_lines{LOG_MAX_LINES};     //单个文件的行数上限,0兜回LOG_MAX_LINES
        uint32_t              flush_interval_ms{LOG_FLUSH_INTERVAL_MS};//定时刷盘间隔,0表示关闭定时刷盘
    };
private:
    //一条待处理的日志:业务线程只填元信息与"把正文延迟格式化出来"的可调用对象
    struct Record
    {
        Level                        level{Level::INFO};
        bool                         to_console{false};
        bool                         to_file{false};
        int                          thread_index{0};
        long                         os_tid{0};     //真实的OS线程号,必须在业务线程侧取(后台线程取到的是它自己)
        uint32_t                     line{0};
        std::time_t                  time_sec{0};
        int32_t                      time_nsec{0};
        const char*                  file{"?"};    //source_location保证指向静态字符串
        std::function<void(std::string&)> body;    //后台线程调用它才做std::format,追加进调用方给的缓冲
    };
private:
    //文件状态与按秒时间戳缓存:只有后台线程(以及退出前的收尾)会碰,统一由_write_mtx保护
    int                                             _fd=-1;
    std::string                                     _out;                                                   //攒着还没写出去的日志块,由_write_mtx保护
    std::string                                     _out_abandoned;                                         //fork时把子进程继承到的_out挪到这里:既不让子进程再用那块可能不自洽的缓冲,也不让它变成"不可达的内存"(LSan会把不可达的算成泄漏)
    uint32_t                                        _current_line=0;
    int                                             _file_index=0;                                          //当前日志文件的编号
    std::time_t                                     _tm_sec=-1;                                             //时间戳缓存对应的整秒(-1做哨兵:0恰好是epoch,会让时间戳为0的日志复用到全零的_tm)
    std::tm                                         _tm{};                                                  //该整秒的本地时间
    std::filesystem::path                           _log_path;
    std::recursive_mutex                            _write_mtx;                                             //递归锁:用户的formatter里重入日志时不会自己把自己锁死
    std::atomic_uint32_t                            _max_line{LOG_MAX_LINES};                               //单个文件日志行数上限
    //双缓冲:业务线程填_front,后台线程取_back;交接必须等后台把上一块处理完(背压),所以同时最多只有两块缓冲
    std::vector<Record>                             _front;
    std::vector<Record>                             _back;
    bool                                            _writing{false};                                        //后台正在锁外格式化+写盘
    //交接/完成序号。flush()与ERROR/FATAL不能等"后台恰好空闲"(_writing==false且_back空) ——
    //持续压测时生产者总会抢在那个空隙里塞进下一块,等待方可能饿上好几秒。改成等"我交接的那一批已经写完"
    uint64_t                                        _handed{0};                                             //已经交接给后台的批次数
    uint64_t                                        _done{0};                                               //后台已经处理完的批次数
    std::mutex                                      _buf_mtx;                                               //保护_front/_back/_writing
    //后台要等"新缓冲/到点/停止"三种事件,带stop_token的等待只有condition_variable_any支持,
    //而且它能在收到停止请求时立刻唤醒,不会出现"设了停止却还睡满一个间隔"
    std::condition_variable_any                     _buf_cv;
    std::condition_variable                         _space_cv;                                              //业务线程等"后台腾出另一块缓冲"
    std::condition_variable                         _drained_cv;                                            //flush()与ERROR/FATAL等"队列被处理完"
    std::atomic_bool                                _backend_up{false};                                     //后台线程是否在运行
    std::atomic_bool                                _shutdown{false};                                       //正在析构:log()/flush()立刻返回,等待者也立刻放弃等待
    std::jthread                                    _backend;                                               //后台线程:格式化+输出+写盘
    std::atomic_uint32_t                            _flush_interval_ms{LOG_FLUSH_INTERVAL_MS};
    //间隔版本号:set_flush_interval_ms()每次递增。带stop_token的wait_for必须给谓词,而谓词为假时notify只会让等待"继续睡",
    //用版本号把"间隔被改过"表达成谓词,notify才不会被吞掉
    std::atomic_uint64_t                            _flush_interval_epoch{0};
    std::atomic<Level>                              _console_level{Level::INFO};                            //控制台最低等级
    std::atomic<Level>                              _file_level{Level::INFO};                               //日志文件最低等级
    std::atomic_bool                                _console_enabled{true};
    std::atomic_bool                                _color_enabled{false};                                  //控制台是否上色(stdout是终端时自动打开)
    std::atomic_bool                                _show_os_tid{false};                                    //是否在[tN]后面附带真实的OS线程号
    std::atomic_uint64_t                            _short_writes{0};                                       //一次write没写完、循环补齐的次数
    std::atomic_uint64_t                            _write_errors{0};                                       //彻底写失败的次数
    std::atomic_uint64_t                            _format_errors{0};                                      //正文格式化抛异常、被兜底文本替换的行数
    std::atomic_uint64_t                            _rotated_writes{0};                                     //因日志文件打不开而被丢弃的行数
    bool                                            _open_failed_reported{false};                           //打开失败是否已经报过stderr(只跟_write_mtx走)
    int64_t                                         _open_retry_ns{0};                                      //打开失败后的重试时刻:目录不可写时不能每条日志都去open一次
    //被LOG_BUF_SIZE截断的行数:vformat给出的是未截断长度,截断因此变成可观测指标
    std::atomic_uint64_t                            _truncated_lines{0};
    //线程短编号分配器。必须放在类里而不是log()内部:log()是模板,函数体内的局部static会随每个Args...实例化各存一份,
    //那样同一个线程在不同日志调用点上会拿到互相冲突的tN
    //启动期配置(见pending_config):init()写,getLog()首次构造时消费;只在"第一条日志之前"被读写。
    //_inited是常量初始化(atomic_bool的constexpr构造):即使别的TU在动态初始化期就先打了日志,
    //读到的也是确定的false,于是走Config{}默认值,不会去读一份还没就绪的配置
    inline static std::atomic_bool                   _inited{false};
    inline static Log*                              _instance=nullptr;                                      //单例指针:atfork的handler只能用它,不能调getLog()(那会碰函数局部static的初始化锁)
    inline static std::atomic_int                   _thread_seq{0};
    inline static thread_local int                  _thread_index=-1;
    inline static thread_local long                 _os_tid=0;                                              //gettid缓存:一次syscall,之后每条日志零成本
    //本线程是不是后台线程。用户formatter里重入日志/flush()时靠它避免"等自己"死锁
    inline static thread_local bool                 _in_backend=false;
    //等级方括号在整行里的固定位置:"[%04d-%02d-%02d %02d:%02d:%02d.%03d]"正好25字节,后面紧跟"["+5字节等级+"]"
    //上色直接按这个偏移切,不用去搜方括号
    static constexpr size_t                         LEVEL_COLOR_BEGIN=25;
    static constexpr size_t                         LEVEL_COLOR_END=32;
private:
    Log(const Log&)=delete;
    Log& operator=(const Log&)=delete;
    Log(Log&&)=delete;
    Log& operator=(Log&&)=delete;
    //启动期配置的存放点。为什么用函数局部static而不是类里的静态数据成员:
    //嵌套类型的默认成员初始化器不能在"本类类体"里求值(GCC/Clang都会报
    //"default member initializer for 'Log::Config::path' required before the end of its enclosing class"),
    //而函数体是完整类上下文,放这里既合法,又顺带拿到"首次调用时线程安全初始化"的保证
    static Config& pending_config()
    {
        static Config cfg{};
        return cfg;
    }
    explicit Log(const Config& cfg)
    {
        //路径只在构造时定死一次:之后_log_path不会再变,所以log_path()返回const&是安全的,也不需要额外加锁
        //解析优先级:Log::init(path) > 编译期LOG_DEFAULT_PATH(不支持环境变量,配置来源只有这两个)
        _log_path=cfg.path.empty()?std::filesystem::path(LOG_DEFAULT_PATH):cfg.path;
        _max_line.store(cfg.max_lines==0?LOG_MAX_LINES:cfg.max_lines,std::memory_order_release);//0会让每条日志都换一个文件,兜回默认值
        _console_enabled.store(cfg.console,std::memory_order_release);
        _console_level.store(cfg.console_level.value_or(cfg.level),std::memory_order_release);
        _file_level.store(cfg.file_level.value_or(cfg.level),std::memory_order_release);
        //stdout是终端时默认上色,重定向到文件时自动关闭;显式给了color就按显式的来
        _color_enabled.store(cfg.color.has_value()?*cfg.color:(::isatty(STDOUT_FILENO)!=0),std::memory_order_release);
        _show_os_tid.store(cfg.show_os_tid,std::memory_order_release);
        _flush_interval_ms.store(cfg.flush_interval_ms,std::memory_order_release);
        std::error_code ec;
        std::filesystem::create_directories(_log_path,ec);
        //扫描日志目录中的日志文件,接着已有的最大编号继续,避免覆盖旧日志
        _file_index=scan_max_index();
        //只扫描已有编号,不预建文件(见open_next_file的说明)
        _front.reserve(LOG_RECORDS_PER_BUF);
        _back.reserve(LOG_RECORDS_PER_BUF);
        _out.reserve(LOG_WRITE_BLOCK+LOG_BUF_SIZE);
        try{
            _backend=std::jthread([this](std::stop_token stop){worker_loop(stop);});
            _backend_up.store(true,std::memory_order_release);
        }catch(const std::system_error& e){
            //后台线程起不来:退化成"每条日志当场格式化+同步写",功能不受影响
            std::fprintf(stderr,"[log] cannot start the backend thread (%s), logs will be formatted and written synchronously\n",e.what());
        }
        //单例是immortal的(见getLog),退出期没有析构来做最后冲刷;atexit时后台线程还活着,flush能把残余日志落盘。
        //atexit之后(其它静态对象析构里)打的日志可能来不及写出去,但绝不会是UB——这正是immortal的意义
        std::atexit([](){Log::getLog().flush();});
        //fork之后子进程里没有后台线程(只clone了调用线程),而它可能在fork那一刻正持着_write_mtx/_buf_mtx、或正在改_front/_back。
        //子进程里必须把这两把锁重新初始化(继承来的锁可能属于一个已经不存在、永远不会解锁的线程),
        //同时把_backend_up清掉,让子进程退化成"每条日志当场同步写":既不碰那两块可能处于半改状态的缓冲,也不会等一个不存在的后台线程。
        //父进程不需要任何处理:fork前先拿锁再解锁那套会让递归锁在子进程里少解一层(后台线程可能递归持有),这里改成子进程直接重建
        _instance=this;
        ::pthread_atfork(nullptr,nullptr,&Log::atfork_child);
    }
    //atfork的child handler:只碰纯内存与锁,不分配内存、不打日志
    static void atfork_child(){if(_instance){_instance->fork_child();}}
    void fork_child()
    {
        _backend_up.store(false,std::memory_order_release);
        //不析构旧锁直接重新构造:std::mutex/std::recursive_mutex的构造函数只做pthread_mutex_init,不分配内存
        new(&_write_mtx) std::recursive_mutex();
        new(&_buf_mtx) std::mutex();
        //_out是后台线程正在拼的输出块,子进程继承到的可能是"改到一半"的std::string(内部指针/长度不自洽,再用就是UB)。
        //这里把它整体挪到一个再也不会被使用的成员里:移动只搬内部字段、不解引用那块缓冲,移完_out就是合法的空串;
        //留在_out_abandoned里而不是直接丢掉,是因为"不可达的内存"会被LSan算成泄漏(_fd/_current_line这些POD保持原值无害,最多让子进程继续往同一个文件追加)
        _out_abandoned=std::move(_out);
        //thread_local是"每线程"的,子进程里调用fork的那个线程会继承父进程的缓存值(OS线程号/短编号/后台标记都变了),一并清掉
        _thread_index=-1;
        _os_tid=0;
        _in_backend=false;
        _thread_seq.store(0,std::memory_order_relaxed);//子进程是新的进程:短编号从头开始(tN只在本进程内有意义)
    }
    //扫描日志目录里已有的最大文件编号(多进程/多实例共用同一个日志目录时,用来跳过别人刚建出来的名字)
    int scan_max_index()const
    {
        std::error_code ec;
        int max_index=0;
        //range-for的operator++也可能抛filesystem_error(目录在扫描途中被删/权限变化),构造与is_regular_file传了ec挡不住它;
        //首次getLog()在静态初始化期被调用,异常逃出去就是terminate,所以这里再兜一层:已扫到的编号照样可用
        try{
        for(const auto& entry:std::filesystem::directory_iterator(_log_path,ec))
        {
            if(ec){break;}
            if(!entry.is_regular_file(ec)){continue;}
            const std::string filename=entry.path().filename().string();
            static constexpr std::string_view suffix=".log";
            if(!filename.ends_with(suffix)){continue;}
            const size_t underscore=filename.rfind('_');
            if(underscore==std::string::npos){continue;}
            //必须长得像"YYYY_MM_DD_N.log":日志目录是用户目录,里面随便一个"backup_7.log"都会把编号顶飞(跳空不丢日志,但很费解)
            if(underscore<10){continue;}
            bool prefix_ok=true;
            for(size_t i=0;i<10;i++)
            {
                const char c=filename[i];
                const bool want_underscore=(i==4||i==7);
                if(want_underscore?(c!='_'):!(c>='0'&&c<='9')){prefix_ok=false;break;}
            }
            if(!prefix_ok){continue;}
            const size_t begin=underscore+1;
            const size_t end=filename.size()-suffix.size();
            if(begin>=end){continue;}
            int value=0;
            const auto parsed=std::from_chars(filename.data()+begin,filename.data()+end,value);
            if(parsed.ec==std::errc()&&parsed.ptr==filename.data()+end&&value>max_index){max_index=value;}
        }
        }catch(const std::filesystem::filesystem_error& e){
            std::fprintf(stderr,"[log] cannot fully scan %s (%s), continuing with the indices found so far\n",_log_path.c_str(),e.what());
        }
        return max_index;
    }
    //打开下一个日志文件(序号+1),打不开返回false;调用时机:首次需要写文件级日志、或当前文件行数达到上限。
    //不在启动时预建文件:日志等级过滤掉全部输出时,不应该留下一个空文件误导使用者。
    bool open_next_file()
    {
        if(_fd>=0)
        {
            ::close(_fd);
            _fd=-1;
        }
        struct timespec ts{};
        clock_gettime(CLOCK_REALTIME,&ts);
        std::tm tm_buf{};
        localtime_r(&ts.tv_sec,&tm_buf);
        //安全加固:0644(实际权限会被umask削,比如umask 077会得到0600);O_NOFOLLOW(别人在可写目录里预置软链接时拒绝跟随,否则会以O_APPEND写进对方指定的任意文件,以root运行就是提权);
        //O_EXCL(绝不打开已经存在的文件,名字撞EEXIST就换下一个编号重试);不要用O_NONBLOCK(同步写不需要它,而且会让"写满"更难排查)
        std::filesystem::path file_path;
        for(int attempt=0;attempt<256;attempt++)
        {
            char name[64];
            std::snprintf(name,sizeof(name),"%04d_%02d_%02d_%d.log",tm_buf.tm_year+1900,tm_buf.tm_mon+1,tm_buf.tm_mday,++_file_index);
            file_path=_log_path/name;
            _fd=::open(file_path.c_str(),O_CREAT|O_WRONLY|O_APPEND|O_CLOEXEC|O_NOFOLLOW|O_EXCL,0644);
            if(_fd>=0||errno!=EEXIST){break;}
            //名字被别人占了(多进程/多实例共用日志目录):重扫一遍目录直接跳到最大编号之后,
            //只靠++撞次数的话,撞满就把这一行丢了
            if((attempt&7)==7)
            {
                const int scanned=scan_max_index();
                if(scanned>_file_index){_file_index=scanned;}
            }
        }
        _current_line=0;//_current_line表示"当前文件已经写了多少行"
        if(_fd<0)
        {
            //只报一次:日志目录不可写时不能按条刷stderr(每条日志都刷会把stderr变成放大器)
            if(!_open_failed_reported)
            {
                _open_failed_reported=true;
                std::fprintf(stderr,"[log] cannot open log file %s\n",file_path.c_str());
            }
            return false;
        }
        _open_failed_reported=false;
        return true;
    }
    //后台线程:把一整块Record拼成行并输出(控制台+文件)
    void write_batch(std::vector<Record>& batch)
    {
        if(batch.empty()){return;}
        std::lock_guard<std::recursive_mutex> lock(_write_mtx);
        char line[LOG_BUF_SIZE];
        std::string body;//复用同一块缓冲,不要每条日志都新建一个std::string
        for(const Record& rec:batch)
        {
            body.clear();
            try{
                rec.body(body);
            }catch(const std::exception& e){
                //这里不能用std::format:catch块里再抛异常会直接跳出整个try,把后台线程打挂
                body=std::string("<log format error: ")+e.what()+">";
                _format_errors.fetch_add(1,std::memory_order_relaxed);
            }catch(...){
                body="<log format error>";
                _format_errors.fetch_add(1,std::memory_order_relaxed);
            }
            //拼一行:"[时间戳][等级][tN][文件:行号]: 正文\n";同一秒内的行复用上一次的tm,只做一次时区换算
            if(rec.time_sec!=_tm_sec&&localtime_r(&rec.time_sec,&_tm)!=nullptr){_tm_sec=rec.time_sec;}
            const size_t idx=static_cast<size_t>(rec.level);
            const std::string_view name=idx<level_string.size()?level_string[idx]:"?????";
            const int ms=static_cast<int>(rec.time_nsec/1000000);
            int head=0;
            if(_show_os_tid.load(std::memory_order_relaxed))
            {
                head=std::snprintf(line,sizeof(line),"[%04d-%02d-%02d %02d:%02d:%02d.%03d][%-5.*s][t%d/%ld][%s:%u]: ",_tm.tm_year+1900,_tm.tm_mon+1,_tm.tm_mday,_tm.tm_hour,_tm.tm_min,_tm.tm_sec,ms,static_cast<int>(name.size()),name.data(),rec.thread_index,rec.os_tid,rec.file,rec.line);
            }
            else
            {
                head=std::snprintf(line,sizeof(line),"[%04d-%02d-%02d %02d:%02d:%02d.%03d][%-5.*s][t%d][%s:%u]: ",_tm.tm_year+1900,_tm.tm_mon+1,_tm.tm_mday,_tm.tm_hour,_tm.tm_min,_tm.tm_sec,ms,static_cast<int>(name.size()),name.data(),rec.thread_index,rec.file,rec.line);
            }
            //snprintf返回"本该写入"的长度(负数是编码错误,理论上不会发生),用它判断有没有截断
            if(head<0){head=0;_truncated_lines.fetch_add(1,std::memory_order_relaxed);}//头部作废只落正文,同时计入截断统计
            const size_t head_would_be=static_cast<size_t>(head);
            const size_t pos=std::min(head_would_be,sizeof(line));
            const size_t remain=pos<sizeof(line)?sizeof(line)-pos:0;
            const size_t body_len=std::min(body.size(),remain>1?remain-1:0);//留一个字节放换行符
            if(body_len>0){std::memcpy(line+pos,body.data(),body_len);}
            size_t len=pos+body_len;
            if(len>=sizeof(line)){len=sizeof(line)-1;}//没有位置放换行符时顶掉最后一个字符
            line[len++]='\n';
            if(head_would_be+body.size()+1>sizeof(line)){_truncated_lines.fetch_add(1,std::memory_order_relaxed);}
            //控制台输出:上色只染等级那个方括号(第1个方括号是时间戳)
            if(rec.to_console)
            {
                if(!_color_enabled.load(std::memory_order_relaxed)||len<LEVEL_COLOR_END)
                {
                    std::fwrite(line,1,len,stdout);
                }
                else
                {
                    const std::string_view color=idx<level_console_color.size()?level_console_color[idx]:std::string_view{""};
                    std::fwrite(line,1,LEVEL_COLOR_BEGIN,stdout);
                    std::fwrite(color.data(),1,color.size(),stdout);
                    std::fwrite(line+LEVEL_COLOR_BEGIN,1,LEVEL_COLOR_END-LEVEL_COLOR_BEGIN,stdout);
                    std::fwrite("\033[0m",1,4,stdout);
                    std::fwrite(line+LEVEL_COLOR_END,1,len-LEVEL_COLOR_END,stdout);
                }
                if(rec.level>=Level::FATAL){std::fflush(stdout);}
            }
            //落盘:先按需建文件/轮转,把这一行拼进大块;攒够一块再一次write
            if(rec.to_file)
            {
                if(_fd<0||_current_line>=_max_line.load(std::memory_order_relaxed))
                {
                    flush_out();//换文件前先把上一个文件的残余写出去
                    //打不开文件只丢这一次落盘,控制台已经输出过了
                    //退避:目录不可写/被删时,不能每条日志都去做一次注定失败的open
                    if(_fd<0&&log_now_ns()<_open_retry_ns)
                    {
                        _rotated_writes.fetch_add(1,std::memory_order_relaxed);
                        continue;
                    }
                    if(!open_next_file())
                    {
                        _open_retry_ns=log_now_ns()+1000000000LL;
                        _rotated_writes.fetch_add(1,std::memory_order_relaxed);
                        continue;
                    }
                }
                _current_line++;
                _out.append(line,len);
                if(_out.size()>=LOG_WRITE_BLOCK){flush_out();}
            }
        }
        flush_out();//整块写出去:一条日志一次write太贵(实测约0.4us都是系统调用),拼成64KB一块能省掉绝大部分
    }
    //把攒着的大块写出去(调用方必须持有_write_mtx);普通文件写也可能短写(EINTR/部分写),循环补齐并记账
    void flush_out()
    {
        if(_fd<0){_out.clear();return;}
        size_t done=0;
        while(done<_out.size())
        {
            const ssize_t n=::write(_fd,_out.data()+done,_out.size()-done);
            if(n>0)
            {
                if(static_cast<size_t>(n)<_out.size()-done){_short_writes.fetch_add(1,std::memory_order_relaxed);}
                done+=static_cast<size_t>(n);
                continue;
            }
            if(n<0&&errno==EINTR){continue;}
            _write_errors.fetch_add(1,std::memory_order_relaxed);
            break;
        }
        _out.clear();
    }
    //后台线程主循环:等"新缓冲/到点/停止",把缓冲取走做格式化与落盘
    void worker_loop(std::stop_token stop)
    {
        _in_backend=true;
        std::vector<Record> batch;
        batch.reserve(LOG_RECORDS_PER_BUF);
        for(;;)
        {
            {
                std::unique_lock<std::mutex> lock(_buf_mtx);
                while(_back.empty()&&!stop.stop_requested())
                {
                    const uint32_t interval=_flush_interval_ms.load(std::memory_order_relaxed);
                    if(interval==0)
                    {
                        //关闭了定时刷盘:等"整块好了"/"停止"/"间隔被改过"。每秒兜底重查一次,
                        //谓词里必须带上epoch,否则set_flush_interval_ms(0->非0)的notify会被这个等待吞掉,要等满1秒才生效
                        const uint64_t zero_epoch=_flush_interval_epoch.load(std::memory_order_acquire);
                        (void)_buf_cv.wait_for(lock,stop,std::chrono::seconds(1),[this,zero_epoch]
                        {
                            return !_back.empty()||_flush_interval_epoch.load(std::memory_order_acquire)!=zero_epoch;
                        });
                        continue;
                    }
                    const uint64_t epoch=_flush_interval_epoch.load(std::memory_order_acquire);
                    //谓词里必须带上epoch:否则set_flush_interval_ms()的notify只会把等待者叫醒一次,
                    //发现_back还是空就带着原截止时间接着睡 —— "运行期改小间隔/关掉定时刷盘"要等满旧间隔才生效
                    (void)_buf_cv.wait_for(lock,stop,std::chrono::milliseconds(interval),[this,epoch]
                    {
                        return !_back.empty()||_flush_interval_epoch.load(std::memory_order_acquire)!=epoch;
                    });
                    if(!_back.empty()){break;}//有整块了
                    if(stop.stop_requested()){break;}
                    if(_flush_interval_epoch.load(std::memory_order_acquire)!=epoch){continue;}//间隔被改过:立刻回去重新读,不能顺手把半满的缓冲刷掉
                    if(!_front.empty())//到点:把半满的缓冲也交给后台
                    {
                        _back.swap(_front);
                        ++_handed;
                        break;
                    }
                }
                if(_back.empty())
                {
                    if(_front.empty()){break;}//收到停止且没有残留:退出
                    _back.swap(_front);//收到停止:把剩下的也处理掉
                    ++_handed;
                }
                batch.swap(_back);
                _writing=true;
            }
            try{
                write_batch(batch);
            }catch(...){
                //后台线程里绝不能让异常逃出去:那会直接std::terminate把整个进程带走(bad_alloc之类)
                _write_errors.fetch_add(1,std::memory_order_relaxed);
            }
            batch.clear();
            {
                std::lock_guard<std::mutex> lock(_buf_mtx);
                _writing=false;
                _done=_handed;//这一批(以及它之前交接的所有批次)已经落盘
            }
            _space_cv.notify_all();    //业务线程可以继续用另一块缓冲了
            _drained_cv.notify_all();  //flush()与ERROR/FATAL等待者可以走了
        }
        _in_backend=false;
    }
    //把格式化参数按值搬进Record:const char*/char[]/string_view必须拷成std::string,
    //否则业务线程返回后它们指向的临时对象就没了,后台线程会读到悬空内存
    template<typename T>
    static auto store_arg(T&& value)
    {
        using U=std::decay_t<T>;
        using Raw=std::remove_reference_t<T>;
        //字符串字面量/字符数组的地址不可能为空,这里不要写判空:否则GCC会报-Waddress/-Wnonnull-compare
        if constexpr(std::is_array_v<Raw>&&std::is_same_v<std::remove_cv_t<std::remove_extent_t<Raw>>,char>)
        {
            return std::string(value);
        }
        else if constexpr(std::is_same_v<U,const char*>||std::is_same_v<U,char*>)
        {
            return value?std::string(value):std::string();
        }
        else if constexpr(std::is_same_v<U,std::string_view>)
        {
            return std::string(value);
        }
        else
        {
            return U(std::forward<T>(value));
        }
    }
    //只保留文件名(完整路径太长)
    static const char* trim_file(const char* file)
    {
        if(!file){return "?";}
        const char* slash=std::strrchr(file,'/');
        return slash?slash+1:file;
    }
    //业务线程:把一条记录塞进当前缓冲;满了或者等级是ERROR及以上就与后台交接
    void submit(Record&& rec)
    {
        if(!_backend_up.load(std::memory_order_acquire)||_in_backend)
        {
            //后台没起来(创建失败/正在析构/fork之后),或者本线程就是后台线程(用户formatter里重入日志):
            //当场格式化+同步写。_write_mtx是递归锁,所以后台线程重入时不会自己锁死
            std::vector<Record> one;
            one.reserve(1);
            one.push_back(std::move(rec));
            try{
                write_batch(one);
            }catch(...){
                _write_errors.fetch_add(1,std::memory_order_relaxed);
            }
            return;
        }
        const bool wait_for_it=rec.level>=Level::ERROR;
        std::unique_lock<std::mutex> lock(_buf_mtx);
        if(_shutdown.load(std::memory_order_acquire)){return;}//拿锁的这段时间里可能已经开始析构了
        if(!wait_for_it&&_front.size()<LOG_RECORDS_PER_BUF)
        {
            _front.push_back(std::move(rec));
            return;
        }
        //要交接了:后台还在处理上一块时必须等它把缓冲腾出来(双缓冲的背压);析构开始就放弃这条日志
        while(!_shutdown.load(std::memory_order_acquire)&&(_writing||!_back.empty())){_space_cv.wait(lock);}
        if(_shutdown.load(std::memory_order_acquire)){return;}
        if(!wait_for_it)
        {
            _back.swap(_front);
            ++_handed;
            _buf_cv.notify_one();
            _front.push_back(std::move(rec));
            return;
        }
        //ERROR/FATAL:连同已经攒着的一起交给后台,并等它写完再返回 —— 崩溃/abort前关键日志一定在盘上
        _front.push_back(std::move(rec));
        _back.swap(_front);
        ++_handed;
        const uint64_t target=_handed;
        _buf_cv.notify_one();
        while(!_shutdown.load(std::memory_order_acquire)&&_done<target){_drained_cv.wait(lock);}
    }
public:
    ~Log()
    {
        //先让后台线程退出,再做最后一次收尾(顺序不能反:它也要拿那把锁)。
        //_shutdown先置true:此后log()/flush()立刻返回;正在等锁的线程也要被叫醒并放弃等待 ——
        //否则"静态对象析构时打日志"或"退出时还有线程在打日志"会碰到已经拆掉的对象(use-after-free)
        _shutdown.store(true,std::memory_order_release);
        _backend_up.store(false,std::memory_order_release);
        _backend.request_stop();
        _buf_cv.notify_all();
        _space_cv.notify_all();
        _drained_cv.notify_all();
        if(_backend.joinable()){_backend.join();}
        {
            std::lock_guard<std::mutex> lock(_buf_mtx);
            if(!_front.empty()){_back.swap(_front);}
            if(!_back.empty())
            {
                try{
                    write_batch(_back);
                }catch(...){
                    _write_errors.fetch_add(1,std::memory_order_relaxed);
                }
                _back.clear();
            }
        }
        std::lock_guard<std::recursive_mutex> lock(_write_mtx);
        if(_fd>=0)
        {
            ::close(_fd);
            _fd=-1;
        }
    }
    //启动期配置入口:必须在main里、第一条日志之前调用,且早于创建任何线程(即"先init再起线程再打日志")。
    //已经构造出日志器后再调只会往stderr报错并忽略 —— 这正是"路径被首次调用静默锁死"最容易踩的坑,所以让它出声。
    //带path的简写见下面的重载;想改运行期行为就用set_level/set_max_lines等既有接口,不要指望改路径。
    static void init(Config cfg)
    {
        if(_instance)
        {
            std::fprintf(stderr,"[log] logger already built with %s, init() ignored\n",_instance->log_path().c_str());
            return;
        }
        const int saved=errno;
        pending_config()=std::move(cfg);
        //release:把配置的写入发布出去;别的线程acquire到这个true时一定能看见完整配置
        _inited.store(true,std::memory_order_release);
        Log::getLog();    //立刻构造:让配置当场确定,而不是拖到第一条日志
        errno=saved;
    }
    static void init(){init(Config{});}                                       //等价于用全部默认值初始化
    static void init(std::filesystem::path path){init(Config{.path=std::move(path)});}
    static Log& getLog()
    {
        //首次调用会惰性构造日志单例(建目录、扫已有编号、起后台线程,一堆syscall会把errno改掉),
        //而调用方经常就在同一行里用ERROR("...: {}",errno_text(errno)) —— 所以必须原样保护errno,
        //否则"启动时绑不上端口"这种最需要看清原因的场景,日志里的errno会是错的(实测会报成EINVAL)
        const int saved=errno;
        //单例永不析构(immortal):静态析构期打日志与~Log()并发是关不掉的竞态
        //(log()查完_shutdown被抢占、析构跑完、然后submit()摸到已销毁的锁/在析构中重建日志文件),
        //泄漏一个单例换"任意时刻打日志都不是UB";退出期的落盘由atexit里的flush()尽力保证。
        //配置来源:_inited为true才去读pending_config()(见_inited的声明注释:不无条件读,免得在配置就绪前碰到它)
        static Log* asynclog=new Log(_inited.load(std::memory_order_acquire)?pending_config():Config{});
        errno=saved;
        return *asynclog;
    }
    //把已经排队的日志全部写出去(进程退出前,或者想立刻看到日志时调用)
    void flush()
    {
        //正在析构,或者本线程就是后台线程(用户formatter里调了flush):立刻返回,绝不能等自己
        if(_shutdown.load(std::memory_order_acquire)||_in_backend){return;}
        if(!_backend_up.load(std::memory_order_acquire))
        {
            //没有后台线程时每条日志都是当场同步写的:拿一次写锁,保证"正在写的那些"已经写完
            std::lock_guard<std::recursive_mutex> lock(_write_mtx);
            return;
        }
        std::unique_lock<std::mutex> lock(_buf_mtx);
        if(!_front.empty())
        {
            while(!_shutdown.load(std::memory_order_acquire)&&(_writing||!_back.empty())){_space_cv.wait(lock);}
            if(!_shutdown.load(std::memory_order_acquire))
            {
                _back.swap(_front);
                ++_handed;
                _buf_cv.notify_one();
            }
        }
        const uint64_t target=_handed;
        while(!_shutdown.load(std::memory_order_acquire)&&_done<target){_drained_cv.wait(lock);}
    }
    //最低输出等级:控制台与文件同时生效
    void set_level(Level level)
    {
        set_console_level(level);
        set_file_level(level);
    }
    Level get_level()const
    {
        Level c=_console_level.load(std::memory_order_acquire);
        Level f=_file_level.load(std::memory_order_acquire);
        return c<f?c:f;
    }
    //只改控制台:低于该等级的不打印到stdout
    void set_console_level(Level level){_console_level.store(level,std::memory_order_release);}
    Level get_console_level()const{return _console_level.load(std::memory_order_acquire);}
    //只改文件:低于该等级的不写入日志文件
    void set_file_level(Level level){_file_level.store(level,std::memory_order_release);}
    Level get_file_level()const{return _file_level.load(std::memory_order_acquire);}
    void set_console(bool enable){_console_enabled.store(enable,std::memory_order_release);}
    bool get_console()const{return _console_enabled.load(std::memory_order_acquire);}
    void set_color(bool enable){_color_enabled.store(enable,std::memory_order_release);}
    bool get_color()const{return _color_enabled.load(std::memory_order_acquire);}
    //是否在[tN]后面附带真实OS线程号(排查CPU占用时有用)
    void set_show_os_tid(bool enable){_show_os_tid.store(enable,std::memory_order_release);}
    bool get_show_os_tid()const{return _show_os_tid.load(std::memory_order_acquire);}
    //改轮转行数上限:先把已经排队的日志按旧上限写完,新上限只影响之后写入的行
    //(否则"排队的日志"会被后台线程按新上限处理,调用方看到的轮转时机就不是它设的那个了)
    void set_max_lines(uint32_t lines)
    {
        if(lines==0){return;}
        flush();
        _max_line.store(lines,std::memory_order_release);
    }
    uint32_t get_max_lines()const{return _max_line.load(std::memory_order_relaxed);}
    //攒批的兜底刷盘间隔(毫秒);0表示关闭定时刷盘(退回纯双缓冲)
    void set_flush_interval_ms(uint32_t ms)
    {
        _flush_interval_ms.store(ms,std::memory_order_release);
        _flush_interval_epoch.fetch_add(1,std::memory_order_release);
        std::lock_guard<std::mutex> lock(_buf_mtx);
        _buf_cv.notify_all();//让新间隔立刻生效
    }
    uint32_t flush_interval_ms()const{return _flush_interval_ms.load(std::memory_order_acquire);}
    //一次write没写完、由循环补齐的次数(有短写不代表丢日志)
    uint64_t short_writes()const{return _short_writes.load(std::memory_order_relaxed);}
    //彻底写失败的次数(正常应该一直是0;不为0就说明真的丢日志了)
    uint64_t write_errors()const{return _write_errors.load(std::memory_order_relaxed);}
    //因日志文件打不开而被丢弃的行数:正常应恒为0
    uint64_t rotated_writes()const{return _rotated_writes.load(std::memory_order_relaxed);}
    //正文格式化抛异常、被"<log format error>"替换的行数(正常应为0)
    uint64_t format_errors()const{return _format_errors.load(std::memory_order_relaxed);}
    //因为超过LOG_BUF_SIZE而被截断的日志行数(正常应为0)
    uint64_t truncated_lines()const{return _truncated_lines.load(std::memory_order_relaxed);}
    //当前日志目录(观测/自检用)
    const std::filesystem::path& log_path()const{return _log_path;}
    //写日志入口:业务线程只做过滤+取时间戳+把参数搬进Record,格式化与落盘全部交给后台线程
    template<typename... Args>
    void log(Level level,const std::source_location& loc,std::format_string<Args...> fmt,Args&&... args)
    {
        if(_shutdown.load(std::memory_order_acquire)){return;}//已经在析构:不能再碰这个对象
        //1.日志等级过滤(被过滤掉的日志不付任何格式化/分配代价)
        const bool to_console=_console_enabled.load(std::memory_order_relaxed)&&level>=_console_level.load(std::memory_order_acquire);
        const bool to_file=level>=_file_level.load(std::memory_order_acquire);
        if(!to_console&&!to_file)[[likely]]{return;}
        //2.取元信息:时间戳必须在业务线程取,后台线程取到的是"写盘时刻"而不是"发生时刻"
        Record rec;
        rec.level=level;
        rec.to_console=to_console;
        rec.to_file=to_file;
        struct timespec ts{};
        clock_gettime(CLOCK_REALTIME,&ts);
        rec.time_sec=ts.tv_sec;
        rec.time_nsec=static_cast<int32_t>(ts.tv_nsec);
        if(_thread_index<0){_thread_index=_thread_seq.fetch_add(1,std::memory_order_relaxed);}
        rec.thread_index=_thread_index;
        //os_tid同理必须在业务线程侧取:write_batch跑在后台线程上,在那里gettid()拿到的永远是后台线程号
        if(_show_os_tid.load(std::memory_order_relaxed))
        {
            if(_os_tid==0){_os_tid=::gettid();}
            rec.os_tid=_os_tid;
        }
        rec.line=loc.line();
        rec.file=trim_file(loc.file_name());
        //3.把参数按值搬进Record,std::format留给后台线程
        rec.body=[fmt,...stored=store_arg(std::forward<Args>(args))](std::string& out)mutable
        {
            std::vformat_to(std::back_inserter(out),fmt.get(),std::make_format_args(stored...));
        };
        submit(std::move(rec));
    }
};
//先做一次等级过滤
#define LOG_INTERNAL(level,...)                                                       \
    do{                                                                               \
        if constexpr(static_cast<int>(level)>=LOG_MIN_LEVEL)                          \
        {                                                                             \
            Log::getLog().log(level,std::source_location::current(),__VA_ARGS__);     \
        }                                                                             \
    }while(0)
#define TRACE(format,...) LOG_INTERNAL(Log::Level::TRACE,format,##__VA_ARGS__)
#define DEBUG(format,...) LOG_INTERNAL(Log::Level::DEBUG,format,##__VA_ARGS__)
#define INFO(format,...)  LOG_INTERNAL(Log::Level::INFO, format,##__VA_ARGS__)
#define WARN(format,...)  LOG_INTERNAL(Log::Level::WARN, format,##__VA_ARGS__)
#define ERROR(format,...) LOG_INTERNAL(Log::Level::ERROR,format,##__VA_ARGS__)
#define FATAL(format,...) LOG_INTERNAL(Log::Level::FATAL,format,##__VA_ARGS__)
