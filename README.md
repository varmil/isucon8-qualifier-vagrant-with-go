# vagrant-isucon/isucon8-qualifier-standalone

## OVERVIEW

isucon8予選とほぼ同じ環境を構築するためのVagrantfileです。

## USAGE
- vagrant実行環境を用意する
- Vagrantfileがあるディレクトリで下記
  - `vagrant up`
  - `vagrant ssh`



## realizeでサーバ起動させるための手順
０．（済）
```
# Vagrantfileに追記。rsyncでないとファイル変更検知不可。（app.goがVagrantfileと同じ階層にある前提）
config.vm.synced_folder "./", "/home/isucon/torb/webapp/go/src/torb/", type: "rsync"
```

１．（済）
```
# app.goのmain()の頭の方に下記を追加

    {
        err := godotenv.Load("../env.sh")
        if err != nil {
            log.Fatal("Error loading .env file")
        }
    }
```

２．
```
# ホストのターミナルで新しくタブを開いて下記を実行
vagrant rsync-auto
```


３．
```
# VM内でのコマンド
sudo -i -u isucon

### デーモンを止める
sudo systemctl stop    torb.perl
sudo systemctl disable torb.perl
sudo systemctl stop torb.go
sudo systemctl disable torb.go

### install the packages
go get -u github.com/oxequa/realize github.com/joho/godotenv

### サーバ起動（停止は ctrl + c）
cd /home/isucon/torb/webapp/go
GOPATH=`pwd`:`pwd`/vendor:/home/isucon/go realize s --no-config --path="./src/torb" --run
```
[参照実装の切り替え方法](https://github.com/isucon/isucon8-qualify/blob/master/doc/MANUAL.md#%E5%8F%82%E7%85%A7%E5%AE%9F%E8%A3%85%E3%81%AE%E5%88%87%E3%82%8A%E6%9B%BF%E3%81%88%E6%96%B9%E6%B3%95) も参照のこと



## pprof設定
１．（済）
```
# app.goのmain()の先頭に記述

    go func() {
        log.Println(http.ListenAndServe(":6060", nil))
    }()
```

２．
```
# VM
sudo systemctl stop firewalld
sudo systemctl disable firewalld

go get -u github.com/google/pprof
sudo yum install -y graphviz

# 30秒間プロファイリングし、その後HTTPサーバを8888で起動
pprof -http="0.0.0.0:8888" localhost:6060/debug/pprof/profile
```



## RUN BENCH
```
sudo -i -u isucon
cd torb/bench
bin/bench -remotes=127.0.0.1 -output result.json
```



## DEPENDENCY
```
sudo -i -u isucon
go get -u github.com/joho/godotenv
go get github.com/thoas/go-funk
go get github.com/patrickmn/go-cache
```



## 動作確認

macOS + VirtualBox 5.2.18 + Vagrant 2.1.5で動作確認済

## 本来の設定と異なるところ

本来のサーバは(CPU 2コア、メモリ1GB)の3台構成です。



## FAQ

### プログラムの動かし方がわからない

以下を確認ください。

- [ISUCON8予選問題](https://github.com/isucon/isucon8-qualify)

### ブラウザで動作確認ができない

`http://192.168.33.18/` にアクセスしてください。
