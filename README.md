# vagrant-isucon/isucon8-qualifier-standalone

## OVERVIEW

isucon8予選とほぼ同じ環境を構築するためのVagrantfileです。

## USAGE
- vagrant実行環境を用意する
- Vagrantfileがあるディレクトリで下記
  - `vagrant up`
  - `vagrant rsync-auto` (※ターミナルをもう1枚開いて)
  - `vagrant ssh`



## RUN BENCH
```
sudo -i -u isucon
cd torb/bench
bin/bench -remotes=127.0.0.1 -output result.json
```



## DEPENDENCY
```
sudo -i -u isucon
go get -u github.com/oxequa/realize
go get -u github.com/joho/godotenv
```



## HOW TO SERVE
```
sudo -i -u isucon
cd /home/isucon/torb/webapp/go
GOPATH=`pwd`:`pwd`/vendor:/home/isucon/go realize s --no-config --path="./src/torb" --run
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

### Vagrantがない環境で試したい

[isucon/isucon8-qualify](https://github.com/isucon/isucon8-qualify)をご利用ください。
