package jp.howan.songdata;

import com.sun.jna.Native;
import com.sun.jna.WString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.logging.Logger;

/**
 * EverythingDirect
 * Direct Mapping（JNA Native.register）で Everything SDK の主要関数を呼び出すためのユーティリティ。
 * 動作前提
 * - 実行時のカレントディレクトリ（System.getProperty("user.dir")）配下にある
 *   "natives/Everything64.dll" または "natives/Everything32.dll" を参照してロードします。
 * - DLL が見つからない／ロードできない場合は isAvailable() が false を返し、呼び出しはフォールバック可能です。
 * 使い方（例）:
 *   if (EverythingDirect.isAvailable()) {
 *       List<Path> p = EverythingDirect.doSearchCollect("parent:\"D:\\BMS\\\" ext:bms;bme count:1");
 *   }
 */
public final class EverythingDirect {
    // ロード成功フラグ（static 初期化で設定される）
    private static volatile boolean loaded = false;

    static {
        try {
            String userDir = System.getProperty("user.dir", ".");

            String dll64 = "Everything64.dll";

            Path nativesDir = Paths.get(userDir, "natives");
            Path dllPath = null;

            if (Files.isDirectory(nativesDir)) {
                dllPath = nativesDir.resolve(dll64);
            }

            if (dllPath != null) {
                // 直接絶対パスで登録（Direct mapping）
                Native.register(dllPath.toAbsolutePath().toString());
                // 軽いプローブ: バージョンを取得してみる
                int major = Everything_GetMajorVersion();
                int minor = Everything_GetMinorVersion();
                if (major > 0 || minor > 0) {
                    Logger.getGlobal().info("[EverythingDirect] Everything64.dll がロードされました from: " + dllPath.toAbsolutePath());
                    loaded = true;
                } else {
                    Logger.getGlobal().severe("[EverythingDirect] Everything との疎通に失敗しました Everything は無効です");
                }
            } else {
                // 見つからない場合は loaded=false としてフォールバック可能にする
                loaded = false;
                Logger.getGlobal().severe("[EverythingDirect] natives/Everything64.dll が作業ディレクトリで見つかりません Everything は無効です");
            }
        } catch (Throwable t) {
            // ロード失敗時は握って loaded=false
            loaded = false;
            Logger.getGlobal().severe("[EverythingDirect] Everything64.dll をロードできませんでした: " + t);
        }
    }

    private EverythingDirect() {
        // util class
    }

    // -------------------- Direct mapping native declarations --------------------
    // NOTE: これらのシグネチャは、Everything SDKでエクスポートされた関数と一致する必要があります。
    private static native int Everything_GetMajorVersion();
    private static native int Everything_GetMinorVersion();
    public static native void Everything_SetSearchW(WString search);

    /**
     * QueryW の wait パラメータ
     * true: 完了まで待つ
     * false: 即時に返す（現在のインデックスに基づく結果）
     */
    public static native boolean Everything_QueryW(boolean wait);

    public static native int Everything_GetNumResults();

    /**
     * Everything_GetResultFullPathNameW
     *  - index: 0-based index
     *  - buf: wide-char バッファ (char[])
     *  - bufSize: バッファ長（char 単位）
     *  戻り値: 実際に書き込まれた文字数 (terminated length)
     */
    public static native int Everything_GetResultFullPathNameW(int index, char[] buf, int bufSize);

    /**
     * Everything_GetResultDateModified
     * 戻り: FILETIME 相当の 64bit 値（100ns 単位、1601基準）
     */
    public static native long Everything_GetResultDateModified(int index);

    public static native int Everything_GetLastError();

    public static native boolean Everything_IsDBLoaded();

    public static native void Everything_SetMatchCase(boolean enable);

    // -------------------- Utilities --------------------

    /**
     * ライブラリがロードされているかどうか。
     */
    public static boolean isAvailable() {
        return loaded;
    }

    /**
     * 指定した Everything 検索クエリで検索し、フルパスの List を返す。
     * wait=true でクエリ完了を待ち、false なら即時のインデックス結果を返す。
     */
    public static List<Path> doSearchCollect(String search) {
        if (!isAvailable()) return Collections.emptyList();
        Objects.requireNonNull(search);

        try {
            // match case を無効化して case-insensitive にしておく（必要なら省く）
            try { Everything_SetMatchCase(false); } catch (Throwable ignore) {}

            Everything_SetSearchW(new WString(search));
            boolean ok = Everything_QueryW(true);
            if (!ok) {
                int err = Everything_GetLastError();
                Logger.getGlobal().severe("[EverythingDirect] QueryW returned false. err=" + err + " query=" + search);
                // QueryW false でも Everything_GetLastError が 0 の場合があるため空リストでフォールバック
                return Collections.emptyList();
            }

            int n = Everything_GetNumResults();
            if (n <= 0) return Collections.emptyList();

            List<Path> out = new ArrayList<>(n);
            final int BUF = 4096;
            char[] buf = new char[BUF];

            for (int i = 0; i < n; i++) {
                int r = Everything_GetResultFullPathNameW(i, buf, buf.length);
                if (r <= 0) continue;

                if (r >= buf.length) {
                    char[] big = new char[r + 2];
                    int r2 = Everything_GetResultFullPathNameW(i, big, big.length);
                    if (r2 > 0) out.add(Paths.get(new String(big, 0, r2)));
                } else {
                    out.add(Paths.get(new String(buf, 0, r)));
                }
            }

            return out;
        } catch (UnsatisfiedLinkError e) {
            // ライブラリがロードできていない場合は空リストでフォールバック
            return Collections.emptyList();
        } catch (Throwable t) {
            t.printStackTrace();
            return Collections.emptyList();
        }
    }

    /**
     * 検索結果のパスと更新日時 (Instant) を含むクラス
     *
     * @param lastModified null あり得る
     */
        public record SearchResult(Path path, Instant lastModified) {

        @Override
            public String toString() {
                return "SearchResult{" + path + ", lastModified=" + lastModified + "}";
            }
        }

    /**
     * 検索クエリでパスと更新日時をまとめて取得するヘルパ
     */
    public static List<SearchResult> doSearchCollectWithDates(String search) {
        if (!isAvailable()) return Collections.emptyList();
        Objects.requireNonNull(search);

        try {
            try { Everything_SetMatchCase(false); } catch (Throwable ignore) {}

            Everything_SetSearchW(new WString(search));
            boolean ok = Everything_QueryW(true);
            if (!ok) {
                int err = Everything_GetLastError();
                Logger.getGlobal().severe("[EverythingDirect] QueryW returned false. err=" + err + " query=" + search);
                return Collections.emptyList();
            }

            int n = Everything_GetNumResults();
            if (n <= 0) return Collections.emptyList();

            List<SearchResult> out = new ArrayList<>(n);
            final int BUF = 4096;
            char[] buf = new char[BUF];

            for (int i = 0; i < n; i++) {
                int r = Everything_GetResultFullPathNameW(i, buf, buf.length);
                Path p = null;
                if (r > 0) {
                    if (r >= buf.length) {
                        char[] big = new char[r + 2];
                        int r2 = Everything_GetResultFullPathNameW(i, big, big.length);
                        if (r2 > 0) p = Paths.get(new String(big, 0, r2));
                    } else {
                        p = Paths.get(new String(buf, 0, r));
                    }
                }

                Instant inst = null;
                try {
                    long filetime100ns = Everything_GetResultDateModified(i);
                    if (filetime100ns != 0L) inst = filetimeToInstant(filetime100ns);
                } catch (Throwable ignore) {
                }

                if (p != null) out.add(new SearchResult(p, inst));
            }

            return out;
        } catch (UnsatisfiedLinkError e) {
            return Collections.emptyList();
        } catch (Throwable t) {
            t.printStackTrace();
            return Collections.emptyList();
        }
    }

    // FILETIME (100 ns since 1601) -> Instant
    private static Instant filetimeToInstant(long filetime100ns) {
        final long WINDOWS_EPOCH_DIFF_100NS = 116444736000000000L; // 1601->1970 diff in 100ns units
        long ms = (filetime100ns - WINDOWS_EPOCH_DIFF_100NS) / 10_000L;
        return Instant.ofEpochMilli(ms);
    }
}

