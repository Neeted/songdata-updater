package jp.howan.songdata;

import java.util.ArrayList;
import java.util.List;

/**
 * Everything の 1.5 (Everything3) を優先し、利用不可なら既存 EverythingDirect (1.4 系) にフォールバックするファサード。
 */
public final class EverythingFacade {

    private EverythingFacade() {
        // static utility
    }

    /**
     * doSearchCollectWithDates: Everything 用の検索を行い、
     * 結果を List<EverythingSearchResult> で返す。
     * 優先: Everything3Direct (1.5) -> fallback: EverythingDirect (既存)
     */
    public static List<EverythingSearchResult> doSearchCollectWithDates(String query) {
        // 1) 可能なら Everything3 (1.5) を優先して試す
        if (Everything3Direct.isAvailable()) {
            try {
                return Everything3Direct.doSearchCollectWithDates(query);
            } catch (Throwable t) {
                // 1.5 呼び出しで何らかの例外が起きたらログを残してフォールバックする
                System.err.println("Everything3Direct failed during search; falling back to legacy EverythingDirect. error=" + t);
                // 続行してフォールバックへ
            }
        }

        // 2) フォールバック: 既存の EverythingDirect (1.4) を使う
        try {
            // 既存の static メソッドをそのまま呼ぶ
            return EverythingDirect.doSearchCollectWithDates(query);
        } catch (Throwable t) {
            // もし既存実装でも例外が出たらログ出力して空リストを返す（呼び出し側は空チェックで対応）
            System.err.println("Legacy EverythingDirect.doSearchCollectWithDates failed: " + t);
            return new ArrayList<>();
        }
    }

    /**
     * EverythingIsAvailable: いずれかの SDK が利用可能なら true を返す。
     */
    public static boolean EverythingIsAvailable() {
        return Everything3Direct.isAvailable() || EverythingDirect.isAvailable();
    }
}
