package jp.howan.songdata;

import com.sun.jna.*;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.logging.Logger;

/**
 * Everything SDK ラッパー
 * - 作業ディレクトリ直下の "natives" フォルダから DLL をロードする想定
 * 参照: <a href="https://www.voidtools.com/support/everything/">Voidtools Everything support</a>
 */
public final class EverythingWrapper {

    public interface EverythingLib extends Library {
        // Unicode / wide-char 版を使う (W suffix)
        void Everything_SetSearchW(WString search);             // NOTE: void (no return)
        boolean Everything_QueryW(boolean wait);                // BOOL return (TRUE/FALSE)
        int Everything_GetNumResults();                         // number of visible results
        int Everything_GetResultFullPathNameW(int index, char[] buf, int bufSize); // returns number of TCHARs copied (excluding null), or 0 on error
        int Everything_GetLastError();                          // extended error code
        void Everything_SetMatchCase(boolean bEnable);          // enable/disable case sensitivity
        // 他に使うなら Everything_GetResultPath / GetResultFileName などを追加
    }

    private final EverythingLib lib;
    private final boolean available;
    private static final int BUF = 8192;

    // 固定拡張子句（毎回コレを組み立てるより文字列で保持）
    private static final String BMS_EXT_CLAUSE = "ext:bms;bme;bml;pms;bmson";
    private static final String AUDIO_EXT_CLAUSE = "ext:wav;ogg;mp3;flac";
    private static final String TXT_EXT_CLAUSE = "ext:txt";

    public EverythingWrapper() {
        EverythingLib tmp = null;
        boolean ok = false;

        // Everything は Windows 専用なので簡易チェック
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (!os.contains("win")) {
            lib = null;
            available = false;
            return;
        }

        // working dir/natives を検索パスに追加しておく（存在すれば）
        try {
            String userDir = System.getProperty("user.dir");
            Path natives = Paths.get(userDir, "natives").toAbsolutePath();
            if (Files.exists(natives) && Files.isDirectory(natives)) {
                try {
                    NativeLibrary.addSearchPath("Everything64", natives.toString());
                    NativeLibrary.addSearchPath("Everything32", natives.toString());
                } catch (Throwable ignore) {}
            }
        } catch (Throwable ignore) {}

        Exception lastEx = null;
        try {
            tmp = Native.load("Everything64", EverythingLib.class);
            ok = probe(tmp);
        } catch (Throwable e64) {
            lastEx = new Exception(e64);
            try {
                tmp = Native.load("Everything32", EverythingLib.class);
                ok = probe(tmp);
            } catch (Throwable e32) {
                lastEx = new Exception(e32);
                tmp = null;
                ok = false;
            }
        }

        if (!ok) {
            if (lastEx != null) Logger.getGlobal().severe("EverythingWrapper: probe failed: " + lastEx.getMessage());
            else Logger.getGlobal().severe("EverythingWrapper: probe failed.");
        } else {
            Logger.getGlobal().info("EverythingWrapper: Everything SDK loaded.");
        }

        lib = tmp;
        available = ok;
    }

    private boolean probe(EverythingLib l) {
        try {
            // 明示的に case-insensitive にして簡単なクエリで probe
            l.Everything_SetMatchCase(false);
            l.Everything_SetSearchW(new WString("ext:__doesnotexist__"));
            return l.Everything_QueryW(true);
        } catch (Throwable t) {
            return false;
        }
    }

    public boolean isAvailable() {
        return available && lib != null;
    }

    /* -------------------------
       High-level helpers for preVisitDirectory
       当初の想定ではこれらのクエリ群を活用する想定だったが、JNAによるネイティブ呼び出しのオーバーヘッドが重く使い物にならない残骸
       ------------------------- */

    /** 直下の BMS 系ファイルを取得（サブディレクトリは含まれない） */
    public List<Path> listBmsFilesDirect(Path dir) {
        if (!isAvailable()) return Collections.emptyList();
        String search = "parent:" + quotedPathWithSlash(dir) + " " + BMS_EXT_CLAUSE;
        return search(search);
    }

    /** 直下の preview*.(wav/ogg/mp3/flac) を先頭1件だけ取得（見つからなければ Optional.empty()） */
    public Optional<Path> findPreviewFileDirect(Path dir) {
        if (!isAvailable()) return Optional.empty();
        // startwith:preview と ext:... を AND、count:1 で最小件数取得
        String search = "parent:" + quotedPathWithSlash(dir) + " startwith:preview " + AUDIO_EXT_CLAUSE + " count:1";
        List<Path> r = search(search);
        if (r.isEmpty()) return Optional.empty();
        return Optional.of(r.get(0));
    }

    /** 直下に .txt があるかどうか（存在チェック） */
    public boolean hasTxtDirect(Path dir) {
        if (!isAvailable()) return false;
        String search = "parent:" + quotedPathWithSlash(dir) + " " + TXT_EXT_CLAUSE + " count:1";
        List<Path> r = search(search);
        return !r.isEmpty();
    }

    /** ファイルの最終更新日時を取得（Files API：I/Oが発生する） */
    public FileTime getLastModifiedTime(Path p) throws IOException {
        return Files.getLastModifiedTime(p);
    }

    /* -------------------------
       Low-level search helper
       ------------------------- */
    /* 普通に以下のコメントアウトしてあるコードで動くが、ChatGPTが謎に堅牢版なるものを出してきたのでそっちを使う(未精査) */
//    private List<Path> search(String search) {
//        List<Path> out = new ArrayList<>();
//        try {
//            // 明示的に case-insensitive にしておく（安全策）
//            try { lib.Everything_SetMatchCase(false); } catch (Throwable ignore) {}
//
//            lib.Everything_SetSearchW(new WString(search));
//            boolean qOk = lib.Everything_QueryW(true);
//            if (!qOk) return Collections.emptyList();
//
//            int n = lib.Everything_GetNumResults();
//            if (n <= 0) return Collections.emptyList();
//
//            char[] buf = new char[BUF];
//            for (int i = 0; i < n; i++) {
//                Arrays.fill(buf, '\0');
//                int r = lib.Everything_GetResultFullPathNameW(i, buf, BUF);
//                if (r > 0) {
//                    String s = Native.toString(buf);
//                    Path p = Paths.get(s);
//                    // safety: skip directories (ext: クエリで返らないはずだが念のため)
//                    try { if (Files.isDirectory(p)) continue; } catch (Throwable ignore) {}
//                    out.add(p);
//                }
//            }
//            return out;
//        } catch (Throwable t) {
//            // エラーが出たら空リストでフォールバック
//            return Collections.emptyList();
//        }
//    }

    /**
     * 生の検索クエリを投げて Path リストを返す
     */
    public List<Path> search(String search) {
        List<Path> out = new ArrayList<>();
        try {
            // 1) 明示的に case-insensitive にする（安全策：SDK のデフォルトが環境で違う可能性あるため）
            try {
                lib.Everything_SetMatchCase(false);
            } catch (Throwable ignore) {
                // 古い SDK 等で未実装なら無視して続行
            }

            // 2) 検索語をセットする（注意: Everything_SetSearchW は void、戻り値をチェックしてはいけない）
            //    → SDK の仕様上、この関数自体は失敗を返さない（ただし内部で例外が出る可能性はある）
            lib.Everything_SetSearchW(new WString(search));

            // 3) 実際に検索を実行する。Everything_QueryW は BOOL を返す（成功: true）
            boolean qOk = lib.Everything_QueryW(true);
            if (!qOk) {
                // Query が false の場合は詳細なエラーコードを取得してログ出力
                int lastErr = 0;
                try { lastErr = lib.Everything_GetLastError(); } catch (Throwable t) {}
                Logger.getGlobal().severe("[Everything] Everything_QueryW returned FALSE for search: " + search);
                Logger.getGlobal().severe("[Everything] Everything_GetLastError() = " + lastErr);
                return Collections.emptyList();
            }

            // 4) 結果件数を取得（visible results）
            int n = lib.Everything_GetNumResults();
            if (n <= 0) return Collections.emptyList();

            // 5) 結果文字列取得のための初期バッファ
            int BUF = 8192;
            char[] buf = new char[BUF];

            for (int i = 0; i < n; i++) {
                // 5-a) 一度目の呼び出し: 返り値 r はコピーした文字数（終端NULL を除く）か、
                //     もし buf が足りない場合、必要な長さを返す実装の場合もある（SDK による）
                int r = lib.Everything_GetResultFullPathNameW(i, buf, buf.length);

                if (r == 0) {
                    // 失敗（例えば invalid call / invalid index） -> ログを取りつつ次へ
                    int lastErr = 0;
                    try { lastErr = lib.Everything_GetLastError(); } catch (Throwable t) {}
                    Logger.getGlobal().severe("[Everything] Everything_GetResultFullPathNameW returned 0 for index " + i + ", err=" + lastErr);
                    continue;
                }

                // 5-b) バッファ不足対応: SDK によっては r >= buf.length を返すことがあるので再確保して再取得
                if (r >= buf.length) {
                    int needed = r + 2; // 少し余裕を持つ
                    char[] bigger = new char[needed];
                    int r2 = lib.Everything_GetResultFullPathNameW(i, bigger, bigger.length);
                    if (r2 <= 0) {
                        int lastErr = 0;
                        try { lastErr = lib.Everything_GetLastError(); } catch (Throwable t) {}
                        Logger.getGlobal().severe("[Everything] re-call GetResultFullPathNameW failed idx=" + i + " err=" + lastErr);
                        continue;
                    }
                    // r2 はコピーされた文字数（終端除く） -> String を作る
                    String pathStr = new String(bigger, 0, r2);
                    try {
                        Path p = Paths.get(pathStr);
                        // 通常 ext: クエリはファイルのみ返すが念のためディレクトリは除外
                        try { if (!Files.isDirectory(p)) out.add(p); } catch (Throwable ignoreStat) { /* skip */ }
                    } catch (Exception ex) { /* skip invalid path */ }
                } else {
                    // 通常ケース: r はコピーされた文字数（終端除く）
                    String pathStr = new String(buf, 0, r);
                    try {
                        Path p = Paths.get(pathStr);
                        try { if (!Files.isDirectory(p)) out.add(p); } catch (Throwable ignoreStat) {}
                    } catch (Exception ex) { /* skip invalid path */ }
                }
            }

            return out;
        } catch (Throwable t) {
            // 例外発生時は空リストでフォールバック（呼び出し元で DirectoryStream に戻す）
            Logger.getGlobal().severe("[Everything] doSearchCollect exception: " + t);
            return Collections.emptyList();
        }
    }

    private static String quotedPathWithSlash(Path dir) {
        String abs = dir.toAbsolutePath().toString().replace("\"", "");
        if (!abs.endsWith("\\") && !abs.endsWith("/")) abs = abs + "\\";
        return "\"" + abs + "\"";
    }
}
