## AEP 性能测试（本地版）

运行命令：

```
./judge.sh <lib-path> <scale of set> <scale of get>
```


## 准备工作

1. 预先编译好KV引擎的链接库
2. judge 仅用于小数据测试，因此key_pool的大小较小，需要手动修改。
