# nginx

```aidl
# 安装nginx
yum install nginx

# 启动配置
/etc/nginx/nginx.conf

# html页面
/usr/share/nginx/html

# 模块
/usr/share/nginx/modules

# 启动nginx (http://hadoop02:80/)
sudo nginx 

# 重启nginx 
sudo nginx -s reload

# 关闭nginx
sudo nginx -s stop 
```

> 控制脚本 ngx
```bash
#!/bin/bash
NGX_HOME=/opt/softwares/nginx-1.12.2
APPNAME=nginx
case $1 in
 "start")
   {
    echo "start nginx[hadoop02]..."
    sudo $NGX_HOME/sbin/nginx
  };;
  "stop")
  {
    echo "stop nginx[hadoop02]..."
    sudo $NGX_HOME/sbin/nginx -s stop
  };;
  "status")
  {
    echo "check nginx[hadoop02]..."
    ssh hadoop02 "ps -ef | grep nginx | grep -v grep | awk '{print \$2}'"
  };;
  "restart")
  {
    echo "start nginx[hadoop02]..."
    sudo $NGX_HOME/sbin/nginx -s reload
  };;
  * )
  {
    echo 'ngx (start | stop | status | restart)'
  };;
esac
```

负载均衡配置

```aidl
user nginx;
worker_processes auto;
error_log /var/log/nginx/error.log;
pid /run/nginx.pid;

# Load dynamic modules. See /usr/share/doc/nginx/README.dynamic.
include /usr/share/nginx/modules/*.conf;

events {
    worker_connections 1024;
}

http {
    log_format  main  '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for"';

    access_log  /var/log/nginx/access.log  main;

    sendfile            on;
    tcp_nopush          on;
    tcp_nodelay         on;
    keepalive_timeout   65;
    types_hash_max_size 2048;

    include             /etc/nginx/mime.types;
    default_type        application/octet-stream;

    include /etc/nginx/conf.d/*.conf;

    # <<<<<
    upstream www.applogs.com {
       server hadoop01:8081 weight=1;
       server hadoop02:8081 weight=2;
       server hadoop03:8081 weight=3;
    }
    # >>>>>

    server {
        listen       80 default_server;
        listen       [::]:80 default_server;
        server_name  _;
        root         /usr/share/nginx/html;

        # Load configuration files for the default server block.
        include /etc/nginx/default.d/*.conf;

        location / {
        }

        # <<<<<
        location /applog {
          proxy_pass http://www.applogs.com;
        }
        # >>>>>

        error_page 404 /404.html;
        location = /404.html {
        }

        error_page 500 502 503 504 /50x.html;
        location = /50x.html {
        }
    }
}
</p>
```

