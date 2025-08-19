package jp.howan.songdata;

import bms.player.beatoraja.Config;
import bms.player.beatoraja.song.SQLiteSongDatabaseAccessor;
import bms.player.beatoraja.song.SongInformationAccessor;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) {
        if (args.length > 0) {
            // jconsole.exeで接続するための一時停止用
            System.out.println("エンターを押すまで待機します");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        }

        if (!Files.exists(Paths.get("config_sys.json"))){
            Logger.getGlobal().info("config_sys.jsonが見つかりません。beatorajaのディレクトリで実行してください。");
            System.exit(1);
        }

        Config config = Config.read();

        System.out.println("対象ルートディレクトリ一覧");
        for (String r : config.getBmsroot()) {
            System.out.println(r);
        }

        SongInformationAccessor infodb = null;
        SQLiteSongDatabaseAccessor songdb = null;
        boolean updateAll = false;
        try {
            infodb = new SongInformationAccessor(config.getSonginfopath());
            songdb = new SQLiteSongDatabaseAccessor(config.getSongpath(), config.getBmsroot());
            Logger.getGlobal().info("song.db更新開始");
            songdb.updateSongDatas(null, config.getBmsroot(), updateAll, infodb);
            Logger.getGlobal().info("song.db更新完了");
        } catch (Exception e) {
            Logger.getGlobal().severe("何らかのエラーでsong.dbの更新ができませんでした:" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
