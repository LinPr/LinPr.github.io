---
title: '常用 Unix 系统调用速查'
date: 2024-02-09T12:08:15+08:00
draft: false
tags: ["linux", "unix", "系统编程"]
categories: ["系统编程"]
author: "LinPr"
description: "常用 Unix 系统调用与 C 库函数速查，按文件、进程、时钟、线程、网络分类。"
---

写 C 时最常翻的那几十个函数，按文件、进程、时钟、线程、网络分五块放着。每条只记签名和一句用途，参数细节、错误码和边界情况还是查 man page。

## 1. 文件操作

分成三块：C 标准库的文件流（`fopen`/`fread`）、目录操作、以及更底层的文件描述符（`open`/`read`/`write`）。日常用标准库就够，需要精细控制时再往下走一层。

### 文件流操作

```c
#include <stdio.h>

打开，关闭
FILE* fopen(const char* filename, const char* mode);
int fclose(FILE* stream);


数据块读写，大小为 size * numb
size_t fread(void *ptr, size_t size, size_t numb, FILE *stream);
size_t fwrite(void *ptr, size_t size, size_t numb, FILE *stream);

格式化读，由format参数指定读的数据格式，由 ... 参数指定接收的容器
int fscanf(FILE *stream, const char *format, …);
int fprintf(FILE *stream, const char *format, ...);


int scanf(const char *format, …);
int printf(const char *format, ...);
//相当于fprintf(stdout,format,…);

从字符串中读取指定的格式
int sscanf(char *str, const char *format, …);
int sprintf(char *str, const char *format, ...);
//eg:sprintf(buf,”the string is;%s”,str);



字符读写
int fgetc(FILE *stream);
int fputc(int c, FILE *stream);
int getc(FILE *stream);//等同于 fgetc(FILE* stream)
int putc(int c, FILE *stream);//等同于 fputc(int c, FILE* stream)
int getchar(void);//等同于 fgetc(stdin);
int putchar(int c);//等同于 fputc(int c, stdout);

单行读写
char *fgets(char *s, int size, FILE *stream);
int fputs(const char *s, FILE *stream);
int puts(const char *s);//等同于 fputs(const char *s,stdout);
char *gets(char *s);//等同于 fgets(const char *s, int size, stdin);

文件定位
int feof(FILE * stream);
//通常的用法为while(!feof(fp))
int fseek(FILE *stream, long offset, int whence);
//设置当前读写点到偏移whence 长度为offset处
long ftell(FILE *stream);
//用来获得文件流当前的读写位置
void rewind(FILE *stream);
//把文件流的读写位置移至文件开头 fseek(fp, 0, SEEK_SET);

文件权限
#include <sys/stat.h>
int chmod(const char* path, mode_t mode);

```





### 目录操作

```c
获取，改变当前目录
#include <unistd.h> //头文件
char *getcwd(char *buf, size_t size); //获取当前目录，相当于pwd命令
getcwd(NULL, 0);
int chdir(const char *path); //修改当前目录，即切换目录，相当于cd命令


创建，删除
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int mkdir(const char *pathname, mode_t mode); //创建目录,mode是目录权限
int rmdir(const char *pathname); //删除目录

其他操作
#include <sys/types.h>
#include <dirent.h>
DIR *opendir(const char *name); //打开一个目录
struct dirent *readdir(DIR *dir); //读取目录的一项信息，并返回该项信息的结构体指针
void rewinddir(DIR *dir); //重新定位到目录文件的头部
void seekdir(DIR *dir,off_t offset);//用来设置目录流目前的读取位置
off_t telldir(DIR *dir); //返回目录流当前的读取位置
int closedir(DIR *dir); //关闭目录文件

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
int stat(const char *pathname, struct stat *buf); //获取文件状态

```



目录项 和 iNode节点

```c
目录项（只保存文件的最基本信息）
struct dirent{
ino_t d_ino; //该文件的inode
off_t d_off; //到下一个dirent的偏移
unsigned short d_reclen;//文件名长度
unsigned char d_type; //所指的文件类型
char d_name[256]; //文件名
};


iNode节点（保存文件的所有信息）
#include <sys/types.h>
#include <dirent.h>
DIR *opendir(const char *name); //打开一个目录
struct dirent *readdir(DIR *dir); //读取目录的一项信息，并返回该项信息的结构体指针
void rewinddir(DIR *dir); //重新定位到目录文件的头部
void seekdir(DIR *dir,off_t offset);//用来设置目录流目前的读取位置
off_t telldir(DIR *dir); //返回目录流当前的读取位置
int closedir(DIR *dir); //关闭目录文件
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>


```



### 文件描述符操作

```c
打开，创建，读写，关闭

#include <sys/types.h> //头文件
#include <sys/stat.h>
#include <fcntl.h>
int open(const char *pathname, int flags); //文件名 打开方式
int open(const char *pathname, int flags, mode_t mode);//文件名 打开方式 权限
int creat(const char *pathname, mode_t mode); //文件名 权限
//creat现在已经不常用了，它等价于
open(pathname,O_CREAT|O_TRUNC|O_WRONLY,mode);
int close(int fd);//fd表示文件描述词,是先前由open或creat创建文件时的返回值。

读写
#include <unistd.h>
ssize_t read(int fd, void *buf, size_t count);//文件描述符 缓冲区 长度
ssize_t write(int fd, const void *buf, size_t count);

改变文件大小
#include <unistd.h>
int ftruncate(int fd, off_t length);

文件映射
void *mmap(void *adr, size_t len, int prot, int flag, int fd, off_t off);
char *p;
p = (char *)mmap(NULL,5,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);

文件定位（内核缓冲区）
off_t lseek(int fd, off_t offset, int whence);//fd文件描述词


获取文件信息
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
int stat(const char *file_name, struct stat *buf); //文件名 stat结构体指针
int fstat(int fd, struct stat *buf); //文件描述词 stat结构体指针


文件描述符的复制
#include <unistd.h>
int dup(int oldfd);
int dup2(int oldfd, int newfd);

```







## 2. 进程操作

主要是进程属性和进程间通信：`fork`/`exec` 一族、`wait` 系列，以及管道、信号、共享内存。

### 进程属性

```c
pid_t getpid(void);
pid_t getppid(void);
uid_t getuid(void);
uid_t geteuid(void);
gid_t getgid(void);
gid_t getegid(void);

pid_t fork(void);

int execl(const char *path, const char *arg0, ... /*, (char *)0 */);
int execv(const char *path, char *const argv[]);
int execle(const char *path, const char *arg0, ... /*, (char *)0, char *const
envp[] */);
int execlp(const char *file, const char *arg0, ... /*, (char *)0 */);
int execvp(const char *file, char *const argv[]);


pid_t wait(int *stat_loc);
pid_t waitpid(pid_t pid, int *stat_loc, int options);



void exit(int status);//可以看出exit函数是由ISO C规定的
void _exit(int status;//_exit是一个Linux系统调用
void _Exit(int status);//_Exit是ISO C规定的库函数
           
pid_t getpgrp(void);//获取进程组ID
pid_t getpgid(pid_t pid);//获取PID为pid的进程的进程组ID，如果pid为0，则获取本进程所属进程组ID
int setpgid(pid_t pid, pid_t pgid);//将pid进程的进程组ID设置为pgid
//如果pid为0，使用调用者的进程ID
//如果pgid为0，则进程组ID和pid一致

pid_t setsid(void); //以当前进程为组长创建一个新会话  
pid_t getsid(pid_t pid); //获取指定进程的会话id

                    
           
```





### 进程间通信

```c
匿名管道
#include <stdio.h>
FILE *popen(const char *command, const char *type);
int pclose(FILE *stream);
           
#include <unistd.h>
int pipe(int pipefd[2]);

有名管道
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
int mkfifo(const char *pathname, mode_t mode);
int rename(const char *oldpath, const char *newpath); //重命名
int unlink(const char *path); //删除硬链接
int link(const char *oldpath, const char *newpath); //创建硬链接
//注意这里的newpath必须是文件名而不是目录名


XSI IPC    
>> ipcs
>> ipcrm -m shmid  
           
共享内存
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
key_t ftok(const char *pathname, int proj_id); //用file 生成key
int shmget(key_t key, size_t size, int shmflg); //创建或者获取一个共享内存区段或者私有共享内存区,如果参数key的取值是宏 IPC_PRIVATE 共享内存段是私有的
void *shmat(int shmid, const void *shmaddr, int shmflg); //连接到一个共享内存区段
int shmdt(const void *shmaddr);
int shmctl(int shmid, int cmd, struct shmid_ds *buf); //修改共享区段属性
使用 shmctl 可以用于对共享内存段执行多种操作。根据cmd参数的不同，可以执行不同的操作：
           IPC_STAT可以用来获取存储共享内存段信息的数据结构；
           IPC_SET可以用来修改共享内存段的所有者、所在组和权限； 
           IPC_RMID可以用来从内核删除共享内存段，当删除时，无论此时有多少进程映射到共享内存段，它都会被标记为待删除，一旦被标记以后，就无法再建立映射了。当最后一个映射解除时，共享内存段就真正被移除。
           

信号量
int semget(key_t key, int nsems, int semflg);
int semctl(int semid, int semnum, int cmd, ...);
在函数 semctl 中，
           semid参数是信号量的标识符，就是 semget 的返回值，
           semnum表示某个信号量值在信号量集合中的索引（范围从0开始）
           cmd参数表示要执行的操作：
           IPC_STAT表示要获取信号量状态，可变参数要设置为状态结构体的指针；
           IPC_SET表示要设置信号量状态，可变参数要设置为状态结构体的指针；
           IPC_RMID表示要删除信号量，不需要设置可变参数，注意和共享内存的删除不同，信号量是立即删除的；
           GETVAL表示获取置某个信号量值，可变参数传入数值
           SETVAL表示设置某个信号量值，可变参数传入数值；
           GETALL和SETALL表示获取和设置信号量集合，可变参数传入一个短整型数组或者不写。
struct sembuf{
    unsigned short sem_num; /* semaphore number */
    short sem_op; /* semaphore operation */
    short sem_flg; /* operation flags */
}
int semop(int semid, struct sembuf *sops, size_t nsops);

//这个由用户自己声明，用于取出信号量的属性时使用
union semun{
    int val; //val for SETVAL
    struct semid_ds *buf; //buffer for IPC_STAT,IPC_SET
    unsigned short *arry; //Array for GETALL,SETALL
}           

           
           
消息队列
int msgget(key_t key, int msgflg);
           
struct mymesg{
    long mtype;
    char mtext[1];
};
int msgsnd(int msqid, const void *msgp, size_t msgsz, int msgflg);
ssize_t msgrcv(int msqid, void *msgp, size_t msgsz, long msgtyp,
int msgflg);
msgctl(msgid,IPC_RMID,NULL);//删除是即时的

           
           
信号
typedef void (*sighandler_t)(int);
注册信号方式1
sighandler_t signal(int signum, sighandler_t handler);

struct sigaction {
    void (*sa_handler)(int);
    void (*sa_sigaction)(int, siginfo_t *, void *);
    sigset_t sa_mask;
    /*
        typedef struct
        {unsigned long int __val[(1024 / (8 * sizeof (unsigned long int)))];} 
        __sigset_t;
        typedef __sigset_t sigset_t; //sigset_t的本质就是一个位图，共有1024位
        int sigemptyset(sigset_t *set); //初始化信号集，清0所有信号
        int sigfillset(sigset_t *set);  //初始化信号集，置1所有信号
        int sigaddset(sigset_t *set, int signum); //增加信号
        int sigdelset(sigset_t *set, int signum); //删除信号
        int sigismember(const sigset_t *set, int signum); //检查信号处于信号集之中
    */
    int sa_flags;
    /*
    	SA_SIGINFO   表示选择sa_sigaction而不是sa_handler作为回调函数
        SA_RESETHAND 处理完捕获的信号以后，信号处理回归到默认，使用情况较少
        SA_NODEFER   解除所有阻塞行为。特别地，执行信号处理流程可以处理同类信号传递，按照栈的方式执行。
        SA_RESTART   让低速系统调用可以自动重启
	*/
    void (*sa_restorer)(void);
};
注册信号方式2
int sigaction(int signum, const struct sigaction *act, struct sigaction
*oldact);

sigset_t pendingSet;
int sigpending(&pendingSet); //获取当前所有未决信号（已经产生没有递送的信号）的集合

使用系统调用 sigprocmask 可以实现全程阻塞的效果
int sigprocmask(int how, const sigset_t *set, sigset_t *oldset); 
           	how 
            SIG_BLOCK 新的屏蔽字是set和当前屏蔽字的并集
            SIG_UNBLOCK 新的屏蔽字是set的补集和当前屏蔽字的交集
            SIG_SETMASK 新的屏蔽字是set
/*
	sigemptyset(&mask);
    sigaddset(&mask,SIGINT);
    int ret = sigprocmask(SIG_BLOCK,&mask,NULL);
    ret = sigprocmask(SIG_UNBLOCK,&mask,NULL);
*/
sigprocmask 可以和  sigpending 之间配合使用
                
                
           
系统调用 
int kill(pid_t pid, int sig);  
int pause(void);  //来阻塞一个进程，直到某个信号被递送时，进程会解除阻塞
/*
	#if 1
        sigprocmask(SIG_UNBLOCK,&mask,NULL);
        pause();//无法就绪
    #else
        sigset_t waitset;
        sigemptyset(&waitset);
        sigsuspend(&waitset);//使用sigsuspend会捕获临界区当中的信号
    #endif

*/

```

## 3. 时钟

Unix 下时间相关的调用，包括墙钟时间、CPU 时间，以及定时器。

```c
#include <sys/time.h>

间隔定时器，类似于相机自动拍摄，当设置拍摄第一张照片的时间后，每间隔一段相同的时间连续自动拍摄（溢出）
struct itimerval 
{
    struct timeval it_interval; /* Interval for periodic timer */
    struct timeval it_value; /* Time until next expiration */  
};

int getitimer(int which, struct itimerval *curr_value);
int setitimer(int which, const struct itimerval *new_value, struct itimerval *old_value);
ITIMER_REAL    会记录真实的时间（也就是时钟时间），当时间到时，会产生一个SIGALRM信号。
ITIMER_VIRTUAL 会记录用户态模式下的CPU时间，当时间到的时候，会产生一个SIGVTALRM信号。
ITIMER_PROF    会记录用户态以及内核态的CPU时间，当时间到的时候，会产生一个SIGPROF信号。
安装信号捕捉handler函数捕捉信号，并且打印时间即可

相关结构体
struct timeval 
{
    time_t tv_sec; /* seconds */
    suseconds_t tv_usec; /* microseconds */
};
struct timezone 
{
    int tz_minuteswest;     /* minutes west of Greenwich */
    int tz_dsttime;         /* type of DST correction */
};
int gettimeofday(struct timeval *tv, struct timezone *tz);
int settimeofday(const struct timeval *tv, const struct timezone *tz);
```

```c
#include <time.h>

time_t time(time_t *tloc);

struct tm 
{
    int tm_sec;    /* Seconds (0-60) */
    int tm_min;    /* Minutes (0-59) */
    int tm_hour;   /* Hours (0-23) */
    int tm_mday;   /* Day of the month (1-31) */
    int tm_mon;    /* Month (0-11) */
    int tm_year;   /* Year - 1900 */
    int tm_wday;   /* Day of the week (0-6, Sunday = 0) */
    int tm_yday;   /* Day in the year (0-365, 1 Jan = 0) */
    int tm_isdst;  /* Daylight saving time */
};
struct tm *gmtime(const time_t *timep);
struct tm *localtime(const time_t *timep);
time_t mktime(struct tm *tm);

ctime(), gmtime() localtime() functions all take an argument of data type  time_t,
    On success, gmtime() and localtime() return a pointer to a struct tm.
    On success, asctime() and ctime() return a pointer to a string.
    

```



## 4. 线程操作

POSIX 线程（pthreads）的创建、同步和退出。

### 错误处理

```c
char *strerror(int errnum);
#define STRERROR_CHECK(ret,msg) {if(ret!=0){fprintf(stderr, "%s:%s\n", msg,strerror(ret));}}


#define PERROR_CHECK(ret , error_val, fileName) { if(ret == error_val){ perror(fileName); return -1; }}


```

### 线程操作

```c
查看线程状态
>> ps -elLf
>> top -H    



创建线程
int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
									void *(*start_routine) (void *), void *arg);

获取线程ID
pthread_t pthread_self(void);

线程退出
void pthread_exit(void *retval);

线程资源回收
int pthread_join(pthread_t thread, void **retval);

线程取消
int pthread_cancel(pthread_t thread);
void pthread_testcancel(void)   //设置cancelation point

线程资源清理
void pthread_cleanup_push(void (*routine)(void *), void *arg);
void pthread_cleanup_pop(int execute);
```





### 互斥锁

```c
pthread_mutex_t fastmutex = PTHREAD_MUTEX_INITIALIZER;
int pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t
*mutexattr);
int pthread_mutex_destroy(pthread_mutex_t *mutex);
int pthread_mutex_lock(pthread_mutex_t *mutex);
int pthread_mutex_unlock(pthread_mutex_t *mutex);
int pthread_mutex_trylock(pthread_mutex_t *mutex);

```



### 条件变量

```c
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
int pthread_cond_init(pthread_cond_t *cond, pthread_condattr_t *cond_attr);
int pthread_cond_signal(pthread_cond_t *cond);
int pthread_cond_broadcast(pthread_cond_t *cond);
int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex);
int pthread_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex, const
struct timespec *abstime);
int pthread_cond_destroy(pthread_cond_t *cond);

```



## 5. 网络编程

从 socket 的创建、绑定、监听到收发，最后是 `epoll` 这套事件驱动接口。



### 地址信息转换

```c
//man 7 ip

套接字二元组
/* Internet address. */
struct sockaddr_in 
{
    sa_family_t sin_family; /* address family: AF_INET */
    in_port_t sin_port; /* port in network byte order */
    
/* struct in_addr { uint32_t s_addr; };*/
    struct in_addr sin_addr; /* internet address */
}    

 /* address in network byte order */
};




#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

字节序转换
//  h:host	    n:net	   l:32bit    	s:16bit
uint32_t htonl(uint32_t hostlong);
uint16_t htons(uint16_t hostshort);
uint32_t ntohl(uint32_t netlong);
uint16_t ntohs(uint16_t netshort);

将IP地址转换成文本
const char *inet_ntop(int af, const void *src,char *dst, socklen_t size); // inet_ntop - convert IPv4 and IPv6 addresses from binary to text form

网络字节序<----->点分十进制相互转换
int inet_aton(const char *cp, struct in_addr *inp);
== in_addr_t inet_addr(const char *cp);

char *inet_ntoa(struct in_addr in);
//线程安全版本是inet_atop inet_ptoa


获取某域名的IP相关信息
#include <netdb.h>
struct hostent *gethostbyname(const char *name); //传入参数
struct hostent 
{
    char *h_name; /* official name of host */
    char **h_aliases; /* alias list */
    int h_addrtype; /* host address type */
    int h_length; /* length of address */
    char **h_addr_list; /* list of addresses */
}

```



### 套接字编程

```c
#include <sys/socket.h>

创建套接字
//domain AF_INET --> IPv4 AF_INET6 --> IPv6
//type SOCK_STREAM --> TCP SOCK_DGRAM --> UDP
//protocol IPPROTO_TCP --> TCP IPPROTO_UDP -->UDP
int socket(int domain, int type, int protocol);

TCP
    
客户端
	1.  int socket(int domain, int type, int protocol);
	2.  int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen);
	3.  ssize_t send(int sockfd, const void *buf, size_t len, int flags);
		ssize_t recv(int sockfd, void *buf, size_t len, int flags);




服务器
	1.  int socket(int domain, int type, int protocol);
	2.  int bind(int sockfd, const struct sockaddr *addr,
socklen_t addrlen);
    3.  int listen(int sockfd, int backlog);
    4.  int accept(int sockfd, struct sockaddr *addr, socklen_t *addrlen);
	5.  ssize_t send(int sockfd, const void *buf, size_t len, int flags);
        ssize_t recv(int sockfd, void *buf, size_t len, int flags);
		

UDP

ssize_t sendto(int sockfd, const void *buf, size_t len, int flags, const struct sockaddr *dest_addr, socklen_t addrlen);

ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr, socklen_t *addrlen);

```

### epoll

```c
1. 创建epoll文件对象
int epoll_create(int size);

2. 设置事件合集
struct epoll_event {
    uint32_t events; /* Epoll events */
    
    epoll_data_t data; /* User data variable */
    /*typedef union epoll_data {
            void *ptr;
            int fd;
            uint32_t u32;
            uint64_t u64;
	} epoll_data_t;*/
};
int epoll_ctl(int epfd, int op, int fd, struct epoll_event* event);

3. 等待事件
int epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);

```
