# songdata-updater

## 何これ

beatorajaの楽曲データベース(songdata.db、songinfo.db)の作成や更新を高速化したものです。
beatorajaが0.8.8の時点で作成し、その時点では十分に高速化できています。
本当に改善出来ていたらbeatorajaにプルリクを出せという話かもしれませんが以下の理由から機能を切り出したアプリとして作っています。

- とりあえず動くようにしただけでJavaのコードとしての品質はあまり考慮しておらずまだテスト中みたいな状態
- バッチを叩けば実行できるCLIアプリとして使いたかった
- 指定したディレクトリ以下を更新といった機能はなく、登録されている全ルートディレクトリでの更新のことしか考えていない
- Everything連携を入れた時点で、マルチプラットフォームなbeatoraja本体とは合わず、ほとんどWindows専用になっている

## 使い方

Liberica JRE同梱版前提です。ほかのJREを使いたい人はsongdata-updater-run.batを書き換えてください。

1. Everythingが常駐していて、BMSデータのあるディレクトリを監視している状態だと最大限にパフォーマンスが発揮されます。(Everythingなくても動くし多少は高速化されています)
2. zipを解凍した中身をbeatorajaと同じフォルダに配置して、songdata-updater-run.batを実行する。以下のようなフォルダの状態するということです。

```ディレクトリ構成
beatoraja
├ beatoraja.jar
├ songdata-updater.jar
├ songdata-updater-run.bat
└ natives
   └ Everything64.dll
```

## 注意

私の環境でしか動作確認できていないのでバグはあるかも知れません。
一応songdata.db、songinfo.dbのバックアップは取ってから実行したほうが良いと思います。
最悪DBが壊れてもお気に入りとタグが消えて再生成に時間がかかるというくらいで済みますが。

## LICENSE

GNU General Public License v3.0