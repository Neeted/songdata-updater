package jp.howan.songdata;

import bms.player.beatoraja.Config;
import bms.player.beatoraja.song.SQLiteSongDatabaseAccessor;
import bms.player.beatoraja.song.SongInformationAccessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    static {
        try {
            FileHandler fileHandler = new FileHandler("songdata-updater_log.xml", false);
            Logger.getGlobal().addHandler(fileHandler);
        } catch (IOException e) {
            Logger.getGlobal().log(Level.SEVERE, "ログファイルの保存先を設定できませんでした", e);
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        if (!Files.exists(Paths.get("config_sys.json"))){
            Logger.getGlobal().warning("config_sys.jsonが見つかりません。beatorajaのディレクトリで実行してください。");
            System.exit(1);
        }
        Config config = Config.read();

        SongInformationAccessor infodb = null;
        SQLiteSongDatabaseAccessor songdb = null;
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
            songdb = new SQLiteSongDatabaseAccessor(config.getSongpath(), config.getBmsroot());
            Logger.getGlobal().info("song.db更新開始");
            songdb.updateSongDatas(null, config.getBmsroot(), updateAll, infodb);
            Logger.getGlobal().info("song.db更新完了");
        } catch (Exception e) {
            Logger.getGlobal().log(Level.SEVERE, "何らかのエラーでsong.dbの更新ができませんでした:", e);
            System.exit(1);
        }
    }
}
