package jp.howan.songdata;

import java.nio.file.Path;

/**
 * 検索結果のパスと更新日時を含むレコードクラス
 * - path: 絶対パスかカレントディレクトリ基準の相対パス
 * - lastModified: 更新日時、UNIX時間(秒)を入れる前提
 */
public record EverythingSearchResult(Path path, long lastModified) {
    @Override
    public String toString() {
        return "SearchResult{" + path + ", lastModified=" + lastModified + "}";
    }
}
