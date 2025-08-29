package jp.howan.songdata;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * BmsFolder: 1フォルダ分の集約情報
 * - bmsFiles: そのフォルダにある BMS パスリスト、bmsrootによって絶対パスか相対パスかを変換済み
 * - hasTxt: そのフォルダに .txt があるかどうか
 * - previewFiles: そのフォルダにある preview*.(wav|ogg|mp3|flac) のパスリスト、bmsrootによって絶対パスか相対パスかを変換済み
 */
public final class BmsFolder {
    public final List<EverythingSearchResult> bmsFiles = new ArrayList<>();
    public boolean hasTxt = false;
    public final List<Path> previewFiles = new ArrayList<>();

    @Override
    public String toString() {
        return "BmsFolder{bms=" + bmsFiles.size() + ", txt=" + hasTxt + ", preview=" + previewFiles.size() + "}";
    }
}
