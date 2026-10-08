#pragma once
#include <algorithm>
#include <charconv>
#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
class ArgParser
{
private:
    enum class Kind
    {
        Flag,       //开关:出现即置true,不取值
        Value,      //带值选项:--name value或--name=value
        Positional  //位置参数:按声明顺序吃裸参数
    };
    using Value=std::variant<bool,int,double,std::string,std::vector<std::string>>;
    struct Option
    {
        std::string                     short_name;         //短名,不含-
        std::string                     long_name;          //长名,不含--
        std::string                     desc;               //帮助里的说明
        std::vector<std::string>        choices;            //可选值范围,为空表示不校验
        Value                           defval;             //注册时的默认值,帮助显示和每次解析复位都用它
        Value                           value;              //当前值
        Kind                            kind=Kind::Value;   //参数种类
        bool                            required=false;     //是否必填
        bool                            variadic=false;     //仅位置参数:把剩余裸参数收进vector
        bool                            set=false;          //本次解析用户是否给出过
    };
private:
    std::string                         _error;             //解析失败原因
    std::vector<Option>                 _options;           //全部选项与位置参数,下标即索引
    std::unordered_map<std::string,int> _index;             //去掉-后的名字到_options下标
    std::vector<int>                    _positional;        //位置参数下标,按声明顺序
    std::size_t                         _next_pos=0;        //下一个待填充的位置参数在_positional里的下标
    std::size_t                         _max_name=0;        //已注册名字的最大长度,用来给组合短开关的匹配范围封顶
    bool                                _help=false;        //本次是否请求了帮助
public:
    const std::string& error()const{return _error;}
    bool help_requested()const{return _help;}
    //添加开关;短名传""表示只支持长名
    ArgParser& add_flag(std::string short_name,std::string long_name,std::string desc)
    {
        Option option;
        option.kind=Kind::Flag;
        option.short_name=normalize(short_name);
        option.long_name=normalize(long_name);
        option.desc=std::move(desc);
        option.value=false;
        register_option(std::move(option));
        return *this;
    }
    //添加带值选项;default_value的类型决定取值类型(bool/int/double/std::string/std::vector<std::string>)
    template<class T>
    ArgParser& add_option(std::string short_name,std::string long_name,std::string desc,T default_value)
    {
        return add_option(std::move(short_name),std::move(long_name),std::move(desc),std::move(default_value),false,{});
    }
    //添加带值选项并标记必填
    template<class T>
    ArgParser& add_option(std::string short_name,std::string long_name,std::string desc,T default_value,bool required)
    {
        return add_option(std::move(short_name),std::move(long_name),std::move(desc),std::move(default_value),required,{});
    }
    //添加带值选项并限定取值范围
    template<class T>
    ArgParser& add_option(std::string short_name,std::string long_name,std::string desc,T default_value,std::initializer_list<std::string> choices)
    {
        return add_option(std::move(short_name),std::move(long_name),std::move(desc),std::move(default_value),false,choices);
    }
    //添加带值选项并限定取值范围;choices是运行期容器时用这个重载
    template<class T>
    ArgParser& add_option(std::string short_name,std::string long_name,std::string desc,T default_value,const std::vector<std::string>& choices)
    {
        return add_option(std::move(short_name),std::move(long_name),std::move(desc),std::move(default_value),false,choices);
    }
    //添加带值选项并同时标记必填与取值范围
    template<class T>
    ArgParser& add_option(std::string short_name,std::string long_name,std::string desc,T default_value,bool required,const std::vector<std::string>& choices)
    {
        add_option_impl(std::move(short_name),std::move(long_name),std::move(desc),make_value(default_value),required,choices);
        return *this;
    }
    //添加位置参数,按声明顺序取值;variadic为true时应作为最后一个位置参数,它会吃掉剩余裸参数
    ArgParser& add_positional(std::string name,std::string desc,bool required=false,bool variadic=false)
    {
        Option option;
        option.kind=Kind::Positional;
        option.long_name=normalize(name);
        option.desc=std::move(desc);
        option.required=required;
        option.variadic=variadic;
        option.value=variadic?Value(std::vector<std::string>()):Value(std::string());
        option.defval=option.value;
        register_option(std::move(option));
        return *this;
    }
    //解析控制:请求帮助时打印帮助并返回true;出错时打印Errors:和帮助并返回false
    bool parse(int argc,const char* const argv[])
    {
        if(argc>0&&argv==nullptr){throw std::invalid_argument("argv is null");}
        std::vector<std::string> args;
        args.reserve((std::size_t)(argc>0?argc:0));
        for(int i=1;i<argc;++i){args.emplace_back(argv[i]!=nullptr?argv[i]:"");}
        return parse(args);
    }
    bool parse(const std::vector<std::string>& args)
    {
        if(parse_impl(args)){return true;}
        std::cerr<<"Errors:\n"<<_error<<"\n\n"<<help();
        return false;
    }
    //取值:类型不符抛std::bad_variant_access,名字未注册抛std::out_of_range
    template<class T>
    T get(const std::string& name) const
    {
        const int idx=find_token(normalize(name),false);
        if(idx<0){throw std::out_of_range("unknown argument:"+normalize(name));}
        return std::get<T>(_options[(std::size_t)idx].value);
    }
    //生成帮助文本:内置部分为英文,用户填的desc原样输出
    std::string help()const
    {
        std::vector<std::pair<std::string,std::string>> rows;
        rows.emplace_back("-h, --help","show this help message and exit");
        for(const Option& option:_options)
        {
            if(option.kind!=Kind::Positional){rows.emplace_back(names(option),describe(option));}
        }
        std::string out="Options:\n"+rows_text(rows);
        rows.clear();
        for(const int idx:_positional)
        {
            const Option& option=_options[(std::size_t)idx];
            rows.emplace_back(option.long_name+(option.variadic?"...":""),describe(option));
        }
        if(!rows.empty()){out+="\nArguments:\n"+rows_text(rows);}
        return out;
    }
private:
    //内部实现:解析主体,只管写_error和返回值,打印交给parse
    bool parse_impl(const std::vector<std::string>& args)
    {
        //每次解析前复位,这样同一个parser可以反复使用
        _error.clear();
        _help=false;
        _next_pos=0;
        for(Option& option:_options)
        {
            option.value=option.defval;
            option.set=false;
        }
        for(std::size_t i=0;i<args.size();++i)
        {
            const std::string& arg=args[i];
            if(arg=="--")
            {
                //--之后全部当位置参数,用于传以-开头的值
                for(std::size_t j=i+1;j<args.size();++j)
                {
                    if(!put_positional(args[j])){return false;}
                }
                break;
            }
            if(arg=="-h"||arg=="--help")
            {
                //碰到帮助立刻打印并结束,不再继续解析后面的参数
                _help=true;
                std::cout<<help();
                return true;
            }
            if(arg.size()>2&&arg[0]=='-'&&arg[1]=='-')
            {
                //长选项:--name或--name=value
                const std::string_view body=std::string_view(arg).substr(2);
                const std::size_t eq=body.find('=');
                const std::string name(body.substr(0,eq));
                const int idx=find_token(name,true);
                if(idx<0)
                {
                    _error=name=="help"?"option --help is a flag and takes no value":"unknown option --"+clip(name);
                    return false;
                }
                Option& option=_options[(std::size_t)idx];
                if(option.kind==Kind::Flag)
                {
                    if(eq!=std::string_view::npos)
                    {
                        _error="option --"+clip(name)+" is a flag and takes no value";
                        return false;
                    }
                    set_flag(option);
                    continue;
                }
                if(eq!=std::string_view::npos)
                {
                    if(!assign_value(option,std::string(body.substr(eq+1)))){return false;}
                    continue;
                }
                if(i+1>=args.size())
                {
                    _error="option --"+clip(name)+" requires a value";
                    return false;
                }
                if(!assign_value(option,args[++i])){return false;}
                continue;
            }
            if(arg.size()>1&&arg[0]=='-'&&is_number(arg)&&find_token(std::string_view(arg).substr(1),true)<0)
            {
                //-1这种负数优先当位置参数,只有真注册了同名短选项才走选项分支
                if(!put_positional(arg)){return false;}
                continue;
            }
            if(arg.size()>1&&arg[0]=='-')
            {
                //短选项:-o value、-ovalue、-o=value,以及组合短开关-abc
                std::string_view body=std::string_view(arg).substr(1);
                std::string_view inline_value;
                bool has_inline=false;
                const std::size_t eq=body.find('=');
                if(eq!=std::string_view::npos)
                {
                    inline_value=body.substr(eq+1);
                    body=body.substr(0,eq);
                    has_inline=true;
                }
                const int whole=find_token(body,true);
                if(whole>=0)
                {
                    //整个body就是一个短名,支持多字符短名
                    Option& option=_options[(std::size_t)whole];
                    if(option.kind==Kind::Flag)
                    {
                        if(has_inline)
                        {
                            _error="option -"+clip(body)+" is a flag and takes no value";
                            return false;
                        }
                        set_flag(option);
                        continue;
                    }
                    if(has_inline)
                    {
                        if(!assign_value(option,std::string(inline_value))){return false;}
                        continue;
                    }
                    if(i+1>=args.size())
                    {
                        _error="option -"+clip(body)+" requires a value";
                        return false;
                    }
                    if(!assign_value(option,args[++i])){return false;}
                    continue;
                }
                for(std::size_t k=0;k<body.size();)
                {
                    //每步按最长匹配取一个短名;匹配长度不必超过已注册名字的最大长度,
                    //否则一条超长的-aaaa...会退化成O(n^2)次查表
                    int idx=-1;
                    std::size_t match_len=1;
                    for(std::size_t len=std::min(body.size()-k,_max_name);len>=1;--len)
                    {
                        const int candidate=find_token(body.substr(k,len),true);
                        if(candidate>=0)
                        {
                            idx=candidate;
                            match_len=len;
                            break;
                        }
                    }
                    if(idx<0)
                    {
                        //-h也是内置开关,所以允许它出现在组合里,比如-vh
                        if(body[k]=='h')
                        {
                            if(has_inline)
                            {
                                _error="option -h is a flag and takes no value";
                                return false;
                            }
                            _help=true;
                            std::cout<<help();
                            return true;
                        }
                        //单个'-'出现在组合里没有意义,加引号避免打印成有歧义的"unknown option --"
                        _error=body[k]=='-'?"unknown option \"-\"":"unknown option -"+std::string(body.substr(k,1));
                        return false;
                    }
                    Option& option=_options[(std::size_t)idx];
                    if(option.kind==Kind::Flag)
                    {
                        set_flag(option);
                        k+=match_len;
                        continue;
                    }
                    //值选项会吃掉剩余字符或内联值,两者都给就是写错了,不能悄悄丢掉一个
                    const std::string_view rest=body.substr(k+match_len);
                    if(!rest.empty())
                    {
                        if(has_inline)
                        {
                            _error="option -"+clip(body)+" has an unexpected value";
                            return false;
                        }
                        if(!assign_value(option,std::string(rest))){return false;}
                    }
                    else if(has_inline)
                    {
                        if(!assign_value(option,std::string(inline_value))){return false;}
                    }
                    else
                    {
                        if(i+1>=args.size())
                        {
                            _error="option -"+std::string(body.substr(k,match_len))+" requires a value";
                            return false;
                        }
                        if(!assign_value(option,args[++i])){return false;}
                    }
                    has_inline=false;
                    break;
                }
                if(has_inline)
                {
                    //整个组合都是开关,却带了=值,直接报错而不是静默忽略
                    _error="option -"+clip(body)+" is a flag and takes no value";
                    return false;
                }
                continue;
            }
            if(!put_positional(arg)){return false;}
        }
        for(const Option& option:_options)
        {
            if(option.required&&!option.set)
            {
                _error=option.kind==Kind::Positional?"missing required argument \""+option.long_name+"\"":"missing required option "+display(option);
                return false;
            }
        }
        return true;
    }
    //内部实现:参数注册与查名
    void add_option_impl(std::string short_name,std::string long_name,std::string desc,Value value,bool required,const std::vector<std::string>& choices)
    {
        Option option;
        option.kind=Kind::Value;
        option.short_name=normalize(short_name);
        option.long_name=normalize(long_name);
        option.desc=std::move(desc);
        option.defval=std::move(value);
        option.value=option.defval;
        option.required=required;
        option.choices=choices;
        //默认值必须落在可选范围内,否则帮助里标出的默认值永远取不到
        if(!option.choices.empty()&&!std::holds_alternative<std::vector<std::string>>(option.defval))
        {
            const std::string canonical=value_text(option.defval);
            bool matched=false;
            for(const std::string& choice:option.choices)
            {
                if(same_text(option.defval,canonical,choice))
                {
                    matched=true;
                    break;
                }
            }
            if(!matched){throw std::invalid_argument("default value not in choices:"+value_text(option.defval));}
        }
        register_option(std::move(option));
    }
    void register_option(Option option)
    {
        if(option.short_name.empty()&&option.long_name.empty()){throw std::invalid_argument("empty argument name");}
        //名字里带=会被当成--name=value的分隔符,注册出来也永远匹配不到,直接拒绝
        if(option.short_name.find('=')!=std::string::npos||option.long_name.find('=')!=std::string::npos){throw std::invalid_argument("argument name contains '='");}
        //-h/--help由parse内置处理,注册同名选项只会被无声吞掉,所以直接拒绝
        //长名h也要拒绝:否则-h打印帮助,而-ah会命中这个长名,行为对不上
        if(option.short_name=="h"||option.long_name=="h"){throw std::invalid_argument("argument name reserved for help:h");}
        if(option.long_name=="help"){throw std::invalid_argument("argument name reserved for help:help");}
        //先查完所有重名再统一入表,否则抛异常时会留下指向不存在选项的索引
        if(!option.short_name.empty()&&_index.count(option.short_name)){throw std::invalid_argument("duplicate argument name:"+option.short_name);}
        if(!option.long_name.empty()&&(option.long_name==option.short_name||_index.count(option.long_name))){throw std::invalid_argument("duplicate argument name:"+option.long_name);}
        if(option.kind==Kind::Positional&&!_positional.empty()&&_options[(std::size_t)_positional.back()].variadic)
        {
            throw std::invalid_argument("positional after variadic:"+option.long_name);
        }
        const int idx=(int)_options.size();
        //先把选项落地再建索引,这样即使后续分配失败也不会出现越界下标
        _options.push_back(std::move(option));
        const Option& added=_options[(std::size_t)idx];
        if(!added.short_name.empty())
        {
            _index.emplace(added.short_name,idx);
            _max_name=std::max(_max_name,added.short_name.size());
        }
        if(!added.long_name.empty())
        {
            _index.emplace(added.long_name,idx);
            _max_name=std::max(_max_name,added.long_name.size());
        }
        if(added.kind==Kind::Positional){_positional.push_back(idx);}
    }
    //解析argv时按精确名字查表:写成--num-或-a-这种就该报错,不能被normalize悄悄接受
    //options_only为true时拒绝用--name匹配位置参数的名字
    int find_token(std::string_view name,bool options_only) const
    {
        const auto it=_index.find(std::string(name));
        if(it==_index.end()){return -1;}
        const int idx=it->second;
        if(options_only&&_options[(std::size_t)idx].kind==Kind::Positional){return -1;}
        return idx;
    }
    //内部实现:解析动作
    void set_flag(Option& option)
    {
        option.value=true;
        option.set=true;
    }
    bool put_positional(const std::string& text)
    {
        if(_positional.empty()||_next_pos>=_positional.size())
        {
            _error="unexpected argument \""+clip(text)+"\"";
            return false;
        }
        Option& option=_options[(std::size_t)_positional[_next_pos]];
        if(!assign_value(option,text)){return false;}
        //variadic位置参数负责吃掉剩余参数,所以不推进游标
        if(!option.variadic){++_next_pos;}
        return true;
    }
    //按默认值定下的类型把文本写进value,顺带做类型与取值范围校验
    bool assign_value(Option& option,const std::string& text)
    {
        const std::string where=option.kind==Kind::Positional?"argument \""+option.long_name+"\"":"option "+display(option);
        if(!option.choices.empty())
        {
            bool matched=false;
            for(const std::string& choice:option.choices)
            {
                if(same_text(option.value,text,choice))
                {
                    matched=true;
                    break;
                }
            }
            if(!matched)
            {
                _error=where+" value \""+clip(text)+"\" not in choices:"+join(option.choices,"/");
                return false;
            }
        }
        if(std::holds_alternative<int>(option.value))
        {
            int parsed=0;
            if(!parse_number(text,parsed))
            {
                _error=where+" value \""+clip(text)+"\" is not an integer";
                return false;
            }
            option.value=parsed;
        }
        else if(std::holds_alternative<double>(option.value))
        {
            double parsed=0;
            if(!parse_number(text,parsed))
            {
                _error=where+" value \""+clip(text)+"\" is not a number";
                return false;
            }
            option.value=parsed;
        }
        else if(std::holds_alternative<bool>(option.value))
        {
            bool parsed=false;
            if(!parse_bool(text,parsed))
            {
                _error=where+" value \""+clip(text)+"\" is not a boolean";
                return false;
            }
            option.value=parsed;
        }
        else if(std::holds_alternative<std::vector<std::string>>(option.value))
        {
            //vector默认值让重复选项按出现顺序累积,用get<std::vector<std::string>>取全部
            std::get<std::vector<std::string>>(option.value).push_back(text);
        }
        else
        {
            option.value=text;
        }
        option.set=true;
        return true;
    }
    //内部实现:帮助与报错文本
    std::string display(const Option& option) const
    {
        if(!option.long_name.empty()){return "--"+option.long_name;}
        return "-"+option.short_name;
    }
    std::string names(const Option& option) const
    {
        std::string out;
        if(!option.short_name.empty()&&!option.long_name.empty()){out="-"+option.short_name+", --"+option.long_name;}
        else if(!option.long_name.empty()){out="    --"+option.long_name;}
        else{out="-"+option.short_name;}
        if(option.kind==Kind::Value){out+=placeholder(option.defval);}
        return out;
    }
    std::string describe(const Option& option) const
    {
        std::string out=option.desc;
        std::vector<std::string> tags;
        if(option.required){tags.emplace_back("required");}
        //必填项不显示默认值,避免和"必须出现"的语义打架
        if(!option.required&&option.kind!=Kind::Flag)
        {
            const std::string text=value_text(option.defval);
            if(!text.empty()){tags.emplace_back("default:"+text);}
        }
        if(option.variadic){tags.emplace_back("repeatable");}
        if(!option.choices.empty()){tags.emplace_back("choices:"+join(option.choices,"/"));}
        if(!tags.empty())
        {
            if(!out.empty()){out+=" ";}
            out+="("+join(tags,",")+")";
        }
        return out;
    }
    std::string rows_text(const std::vector<std::pair<std::string,std::string>>& rows) const
    {
        //按终端列宽对齐,一个非ASCII字符按2列算;desc或默认值里的换行制表符先压成空格,否则表格会串行
        std::size_t width=0;
        for(const auto& row:rows){width=std::max(width,text_width(flatten(row.first)));}
        std::string out;
        for(const auto& row:rows)
        {
            const std::string first=flatten(row.first);
            const std::string second=flatten(row.second);
            out+="  "+first;
            if(second.empty())
            {
                out+="\n";
                continue;
            }
            out.append(width-text_width(first)+2,' ');
            out+=second+"\n";
        }
        return out;
    }
    //内部实现:文本与类型工具
    static std::string normalize(std::string_view name)
    {
        std::size_t begin=0;
        std::size_t end=name.size();
        while(begin<end&&(name[begin]=='-'||name[begin]==' ')){++begin;}
        while(end>begin&&(name[end-1]=='-'||name[end-1]==' ')){--end;}
        return std::string(name.substr(begin,end-begin));
    }
    static std::string join(const std::vector<std::string>& items,const char* sep)
    {
        std::string out;
        for(std::size_t i=0;i<items.size();++i)
        {
            if(i>0){out+=sep;}
            out+=items[i];
        }
        return out;
    }
    //数终端列宽:ASCII算1列,其余(帮助文本里都是中文)算2列
    static std::size_t text_width(std::string_view text)
    {
        std::size_t width=0;
        for(const char raw:text)
        {
            const unsigned char c=static_cast<unsigned char>(raw);
            if((c&0xC0)!=0x80){width+=c<0x80?1:2;}
        }
        return width;
    }
    //帮助是单行表格,把desc和默认值里的换行/制表符换成空格,避免右列串行
    static std::string flatten(std::string text)
    {
        for(char& c:text)
        {
            if(c=='\n'||c=='\r'||c=='\t'){c=' ';}
        }
        return text;
    }
    //报错回显用户输入前截断,否则一条超长参数就能把日志刷爆
    static std::string clip(std::string_view text,std::size_t max_len=64)
    {
        if(text.size()<=max_len){return std::string(text);}
        return std::string(text.substr(0,max_len))+"...";
    }
    //整数与小数共用一套转换:from_chars不受setlocale影响,也不会越界后截断
    template<class T>
    static bool parse_number(std::string_view text,T& out)
    {
        if(!text.empty()&&text.front()=='+')
        {
            text.remove_prefix(1);
            //跳过+之后不允许再来一个符号,否则+-1会被读成-1
            if(text.empty()||text.front()=='+'||text.front()=='-'){return false;}
        }
        if(text.empty()){return false;}
        const std::from_chars_result result=std::from_chars(text.data(),text.data()+text.size(),out);
        return result.ec==std::errc()&&result.ptr==text.data()+text.size();
    }
    static bool parse_bool(std::string_view text,bool& out)
    {
        if(text=="1"||text=="true"||text=="yes"||text=="on")
        {
            out=true;
            return true;
        }
        if(text.empty()||text=="0"||text=="false"||text=="no"||text=="off")
        {
            out=false;
            return true;
        }
        return false;
    }
    static bool is_number(std::string_view text)
    {
        double probe=0;
        return parse_number(text,probe);
    }
    //把默认值收敛到variant的固定类型上,避免const char*被variant选成bool;越界直接报错,不静默截断
    template<class T>
    static Value make_value(const T& value)
    {
        if constexpr(std::is_same_v<std::decay_t<T>,bool>){return value;}
        else if constexpr(std::is_same_v<std::decay_t<T>,std::string>||std::is_convertible_v<std::decay_t<T>,std::string>)
        {
            //默认值传(const char*)nullptr会让std::string构造失败,提前报清楚
            if constexpr(std::is_pointer_v<std::decay_t<T>>||std::is_same_v<std::decay_t<T>,std::nullptr_t>)
            {
                if(value==nullptr){throw std::invalid_argument("default value is null");}
            }
            return std::string(value);
        }
        else if constexpr(std::is_integral_v<std::decay_t<T>>)
        {
            if(std::cmp_less(value,std::numeric_limits<int>::min())||std::cmp_greater(value,std::numeric_limits<int>::max()))
            {
                throw std::invalid_argument("default value out of int range");
            }
            return (int)value;
        }
        else if constexpr(std::is_floating_point_v<std::decay_t<T>>)
        {
            if(value>std::numeric_limits<double>::max()||value<-std::numeric_limits<double>::max())
            {
                throw std::invalid_argument("default value out of double range");
            }
            return (double)value;
        }
        else if constexpr(std::is_same_v<std::decay_t<T>,std::vector<std::string>>){return value;}
        else
        {
            static_assert(!sizeof(std::decay_t<T>),"default value type must be bool,int,double,std::string or std::vector<std::string>");
            return false;
        }
    }
    //默认值转帮助文本
    static std::string value_text(const Value& value)
    {
        if(const std::string* text=std::get_if<std::string>(&value)){return *text;}
        if(const int* number=std::get_if<int>(&value)){return std::to_string(*number);}
        if(const double* number=std::get_if<double>(&value))
        {
            //to_chars是最短往返表示,既不丢精度也不像%g那样截到6位
            char buffer[64]={};
            const std::to_chars_result result=std::to_chars(buffer,buffer+sizeof(buffer),*number);
            return std::string(buffer,result.ptr);
        }
        if(const bool* flag=std::get_if<bool>(&value)){return *flag?"true":"false";}
        return join(std::get<std::vector<std::string>>(value),",");
    }
    //choices比对:字符串按字面量,数值与布尔按解析后的值,这样05/+5/0.50这类同义写法都能对上
    static bool same_text(const Value& value,const std::string& text,const std::string& choice)
    {
        if(std::holds_alternative<int>(value))
        {
            int left=0,right=0;
            return parse_number(text,left)&&parse_number(choice,right)&&left==right;
        }
        if(std::holds_alternative<double>(value))
        {
            double left=0,right=0;
            return parse_number(text,left)&&parse_number(choice,right)&&left==right;
        }
        if(std::holds_alternative<bool>(value))
        {
            bool left=false,right=false;
            return parse_bool(text,left)&&parse_bool(choice,right)&&left==right;
        }
        return text==choice;
    }
    static const char* placeholder(const Value& value)
    {
        if(std::holds_alternative<int>(value)){return " <int>";}
        if(std::holds_alternative<double>(value)){return " <double>";}
        if(std::holds_alternative<bool>(value)){return " <bool>";}
        if(std::holds_alternative<std::vector<std::string>>(value)){return " <value...>";}
        return " <value>";
    }
};