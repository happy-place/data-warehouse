# elasticsearch

查询接口
```aidl
1.借助Spring Initializer构建项目
2.使用Elastic模块，Web模板，spring-boot-starter-parent 需要使用2.1.5.RELEASE才会自动注入JestClient,2.3.2版本的不在自动注入
3.JestClient上面的Autowire报错，可以将Preferences > Editor > Spring Code > Autowire bean class > 报警提升改为warning
4.使用RestController暴露接口服务
```
