package jp.howan.songdata;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.WString;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

/**
 * Everything3Direct
 * Direct Mapping（JNA Native.register）で Everything 1.5 (Everything3_x64.dll) SDK の主要関数を呼び出すためのユーティリティ
 * 動作前提:
 *  - 実行時のカレントディレクトリ (System.getProperty("user.dir")) 配下にある
 *    "natives/Everything3_x64.dll" を参照してロードします。
 *  - DLL が見つからない / ロードできない場合は isAvailable() が false を返します。
 * 実装上の注意:
 *  - Everything3 API は client ポインタ / search_state / result_list を使うため、
 *    JNA ではポインタは Pointer で扱っています。long (ネイティブポインタ長)でも代替可能
 *  - Everything3_GetResultDateModified は UINT64（100ns 単位の FILETIME 相当）を返すため、UNIX時間(秒)に変換して返します。
 */
public final class Everything3Direct {
    private static volatile boolean loaded = false;
    private static volatile Pointer client;

    static {
        try {
            String userDir = System.getProperty("user.dir", ".");
            String dll64 = "Everything3_x64.dll";
            Path nativesDir = Paths.get(userDir, "natives");
            Path dllPath = null;

            if (Files.isDirectory(nativesDir)) {
                dllPath = nativesDir.resolve(dll64);
            }

            if (dllPath != null) {
                // 直接絶対パスで登録（Direct mapping）
                Native.register(dllPath.toAbsolutePath().toString());

                // Connect to Everything3 (NULL instance name -> main instance)
                Pointer client = Everything3_ConnectW(null);
                if (client == null) {
                    client = Everything3_ConnectW(new WString("1.5a"));
                }
                Everything3Direct.client = client;

                // 軽いプローブ: バージョンを取得してみる
                int major = Everything3_GetMajorVersion(Everything3Direct.client);
                int minor = Everything3_GetMinorVersion(Everything3Direct.client);
                int revision = Everything3_GetRevision(Everything3Direct.client);

                // バージョンが取得できていれば疎通成功
                if (major > 0 || minor > 0 || revision > 0) {
                    Logger.getGlobal().info("[Everything3Direct] Everything3_x64.dll がロードされました version: " + major + "." + minor + "." + revision + " from: " + dllPath.toAbsolutePath());
                    loaded = true;
                } else {
                    Logger.getGlobal().severe("[Everything3Direct] Everything1.5 との疎通に失敗しました Everything1.5 は無効です");
                }
            } else {
                // 見つからない場合は loaded=false としてフォールバック可能にする
                loaded = false;
                Logger.getGlobal().severe("[Everything3Direct] natives/Everything3_x64.dll が作業ディレクトリで見つかりません Everything1.5 は無効です");
            }
        } catch (Throwable t) {
            loaded = false;
            Logger.getGlobal().severe("[Everything3Direct] Everything3_x64.dll をロードできませんでした: " + t);
        }
    }

    private Everything3Direct() {
        // util class
    }

    // -------------------- Direct mapping native declarations --------------------
    // NOTE: pointer types are represented as long

    // connection / general
    private static native Pointer Everything3_ConnectW(WString instanceName);
    private static native boolean Everything3_DestroyClient(Pointer client);
    private static native int Everything3_GetMajorVersion(Pointer client);
    private static native int Everything3_GetMinorVersion(Pointer client);
    private static native int Everything3_GetRevision(Pointer client);
    private static native int Everything3_GetLastError(Pointer client);
    private static native int Everything_GetTargetMachine(Pointer client);
    private static native boolean Everything3_IsDBLoaded(Pointer client);

    // search state
    private static native Pointer Everything3_CreateSearchState();
    private static native boolean Everything3_DestroySearchState(Pointer searchState);
    private static native boolean Everything3_SetSearchMatchCase(Pointer searchState, boolean matchCase);
    private static native boolean Everything3_SetSearchTextW(Pointer searchState, WString search);
    private static native boolean Everything3_AddSearchPropertyRequest(Pointer searchState, int propertyId);
    // property id (Everything3.h)
    private static final int EVERYTHING3_PROPERTY_ID_NAME = 0;
    private static final int EVERYTHING3_PROPERTY_ID_PATH = 1;
    private static final int EVERYTHING3_PROPERTY_ID_SIZE = 2;
    private static final int EVERYTHING3_PROPERTY_ID_EXTENSION = 3;
    private static final int EVERYTHING3_PROPERTY_ID_TYPE = 4;
    private static final int EVERYTHING3_PROPERTY_ID_DATE_MODIFIED = 5;
    private static final int EVERYTHING3_PROPERTY_ID_DATE_CREATED = 6;
    private static final int EVERYTHING3_PROPERTY_ID_DATE_ACCESSED = 7;
    private static final int EVERYTHING3_PROPERTY_ID_ATTRIBUTES = 8;
    private static final int EVERYTHING3_PROPERTY_ID_FULL_PATH = 240;
    private static native boolean Everything3_ClearSearchPropertyRequests(Pointer searchState);
    private static native boolean Everything3_SetSearchViewportOffset(Pointer searchState, long offset);
    private static native boolean Everything3_SetSearchViewportCount(Pointer searchState, long count);

    // execute search -> result list (pointer)
    private static native Pointer Everything3_Search(Pointer client, Pointer searchState);
    private static native Pointer Everything3_GetResults(Pointer client, Pointer searchState);
    private static native boolean Everything3_DestroyResultList(Pointer resultList);

    // result list getters
    private static native long Everything3_GetResultListCount(Pointer resultList);
    private static native long Everything3_GetResultListFileCount(Pointer resultList);
    private static native long Everything3_GetResultListFolderCount(Pointer resultList);
    private static native long Everything3_GetResultListTotalSize(Pointer resultList);

    // get result properties / text
    private static native long Everything3_GetResultDateModified(Pointer resultList, long resultIndex); // UINT64
    private static native long Everything3_GetResultDateCreated(Pointer resultList, long resultIndex);
    private static native long Everything3_GetResultDateAccessed(Pointer resultList, long resultIndex);
    private static native long Everything3_GetResultDateRecentlyChanged(Pointer resultList, long resultIndex);

    // full path retrieval
    private static native int Everything3_GetResultFullPathNameW(Pointer resultList, long resultIndex, char[] buf, int bufSize);

    // -------------------- Utilities / wrapper --------------------

    /**
     * ライブラリがロードされているかどうか。
     */
    public static boolean isAvailable() {
        return loaded && client != null;
    }

    /**
     * 検索クエリでパスと更新日時をまとめて取得するヘルパ
     * 既存の EverythingDirect.doSearchCollectWithDates とほぼ同等の動作をするメソッド、戻り値は完全に同等でなければならない
     * @param search Everythingでの検索クエリ文字列
     * @return Path と lastModified(UNIX時間) を持つレコードの List
     */
    public static List<EverythingSearchResult> doSearchCollectWithDates(String search) {
        if (!isAvailable()) return Collections.emptyList();
        Objects.requireNonNull(search);

        Pointer searchState = null;
        Pointer resultList = null;
        try {
            // create search state
            searchState = Everything3_CreateSearchState();
            if (searchState == null) {
                Logger.getGlobal().severe("[Everything3Direct] CreateSearchState failed");
                return Collections.emptyList();
            }

            // set query text
            boolean t = Everything3_SetSearchTextW(searchState, new WString(search));
            if (!t) {
                int err = Everything3_GetLastError(client);
                Logger.getGlobal().severe("[Everything3Direct] SetSearchTextW failed. err=" + err + " query=" + search);
                return Collections.emptyList();
            }

            // request date modified property so that GetResultDateModified returns a meaningful value
            Everything3_ClearSearchPropertyRequests(searchState);
            Everything3_AddSearchPropertyRequest(searchState, EVERYTHING3_PROPERTY_ID_DATE_MODIFIED);
            Everything3_AddSearchPropertyRequest(searchState, EVERYTHING3_PROPERTY_ID_FULL_PATH);

            // execute search (synchronous)
            resultList = Everything3_Search(client, searchState);
            if (resultList == null) {
                int err = Everything3_GetLastError(client);
                Logger.getGlobal().severe("[Everything3Direct] Search returned null. err=" + err + " query=" + search);
                return Collections.emptyList();
            }

            long nLong = Everything3_GetResultListCount(resultList);
            int n = (nLong > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) nLong;
            if (n <= 0) return Collections.emptyList();

            List<EverythingSearchResult> out = new ArrayList<>(n);
            final int BUF = 4096;
            char[] buf = new char[BUF];

            for (int i = 0; i < n; i++) {
                int r = Everything3_GetResultFullPathNameW(resultList, i, buf, buf.length);
                Path p = null;
                if (r > 0) {
                    if (r >= buf.length) {
                        char[] big = new char[r + 2];
                        int r2 = Everything3_GetResultFullPathNameW(resultList, i, big, big.length);
                        if (r2 > 0) p = Paths.get(new String(big, 0, r2));
                    } else {
                        p = Paths.get(new String(buf, 0, r));
                    }
                }

                long unixtime = 0L;
                try {
                    long ft100ns = Everything3_GetResultDateModified(resultList, i);
                    // if zero, maybe property missing -> leave as 0
                    if (ft100ns != 0L) {
                        final long FILETIME_EPOCH_DIFF = 116444736000000000L; // 1601 -> 1970 diff in 100ns units
                        unixtime = (ft100ns - FILETIME_EPOCH_DIFF) / 10_000_000L; // convert 100ns -> UNIX時間
                    }
                } catch (Throwable ex) {
                    // log minimally; don't fail whole search
                    Logger.getGlobal().fine("[Everything3Direct] GetResultDateModified exception: " + ex);
                }

                if (p != null) out.add(new EverythingSearchResult(p, unixtime));
            }

            return out;
        } catch (UnsatisfiedLinkError e) {
            Logger.getGlobal().severe("[Everything3Direct] Native link error: " + e);
            return Collections.emptyList();
        } catch (Throwable t) {
            t.printStackTrace();
            return Collections.emptyList();
        } finally {
            try {
                if (resultList != null) Everything3_DestroyResultList(resultList);
            } catch (Throwable ignore) {}
            try {
                if (searchState != null) Everything3_DestroySearchState(searchState);
            } catch (Throwable ignore) {}
        }
    }

    /**
     * Shutdown を手動で行いたい場合のユーティリティ
     */
    public static void shutdownClient() {
        try {
            if (client != null) {
                Everything3_DestroyClient(client);
                client = null;
            }
        } catch (Throwable ignore) {}
        loaded = false;
    }
}
