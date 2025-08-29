package jp.howan.songdata;

import bms.player.beatoraja.Config;
import bms.player.beatoraja.song.SQLiteSongDatabaseAccessor;
import bms.player.beatoraja.song.SongInformationAccessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.logging.*;

public class Main {

    public static void main(String[] args) {
        // イントロ
        System.out.println("songdata-updater 1.1.0");
//        System.out.println("Everything 1.5 Alpha での動作確認は 1.5.0.1396a (x64) で行いました");
//        System.out.println("Everything での動作確認は 1.4.1.1028 (x64) で行いました");

        // ログの設定
        try {
            Logger.getGlobal().setUseParentHandlers(false);

            FileHandler fileHandler = new FileHandler("songdata-updater_log.xml", false);
            Logger.getGlobal().addHandler(fileHandler);

            ConsoleHandler handler = new ConsoleHandler();
            handler.setFormatter(new LogFormatter());
            handler.setLevel(Level.INFO);

            Logger.getGlobal().addHandler(handler);
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE, "ログファイルの保存先を設定できませんでした", e);
            System.exit(1);
        }

        // コンフィグ読み込み
        if (!Files.exists(Paths.get("config_sys.json"))){
            Logger.getGlobal().warning("config_sys.jsonが見つかりません。beatorajaのディレクトリで実行してください。");
            System.exit(1);
        }
        Config config = Config.read();

        // 以下メイン処理
        SongInformationAccessor infodb;
        SQLiteSongDatabaseAccessor songdb;
        boolean updateAll = false;

        if (args.length > 0) {
            if ("rebuild".equals(args[0])) {
                Logger.getGlobal().info("songdata.dbはfolder/songテーブルをDELETEした上で再構築します");
                updateAll = true;
            } else {
                // jconsole.exeで接続するための一時停止用
                System.out.println("エンターを押すまで待機します");
                Scanner scanner = new Scanner(System.in);
                scanner.nextLine();
            }
        }

        System.out.println("対象ルートディレクトリ一覧");
        for (String r : config.getBmsroot()) {
            System.out.println(r);
        }

        try {
            infodb = new SongInformationAccessor(config.getSonginfopath());
            songdb = new SQLiteSongDatabaseAccessor(config.getSongpath());
            Logger.getGlobal().info("songdata.db / songinfo.db 更新開始");
            songdb.updateSongDatas(null, config.getBmsroot(), updateAll, infodb);
            Logger.getGlobal().info("songdata.db / songinfo.db 更新完了");
        } catch (Exception e) {
            Logger.getGlobal().log(Level.SEVERE, "何らかのエラーでsong.dbの更新ができませんでした:", e);
            System.exit(1);
        }
    }
}

class LogFormatter extends Formatter {
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS")
                             .withZone(ZoneId.systemDefault());
    @Override
    public String format(LogRecord record) {
        // 日時フォーマット
        String time = FORMATTER.format(Instant.ofEpochMilli(record.getMillis()));
        // 呼び出し元
        String source = record.getSourceClassName();
        // スレッドID
        long threadId = record.getLongThreadID();

        // ログ書式
        return String.format("[%s] [%s] (thread:%d) %s [%s]%n",
                time,
                record.getLevel().getName(),
                threadId,
                formatMessage(record),
                source);
    }
}
