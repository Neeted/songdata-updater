package jp.howan.songdata;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * FolderInfo: 1フォルダ分の集約情報
 * - bmsFiles: そのフォルダにある BMS 系ファイルの完全パスリスト
 * - hasTxt: そのフォルダに .txt があるかどうか
 * - previewFiles: そのフォルダにある preview*.(wav|ogg|mp3|flac) の完全パスリスト
 */
public final class FolderInfo {
    public final List<Path> bmsFiles = new ArrayList<>();
    public boolean hasTxt = false;
    public final List<Path> previewFiles = new ArrayList<>();

    @Override
    public String toString() {
        return "FolderInfo{bms=" + bmsFiles.size() + ", txt=" + hasTxt + ", preview=" + previewFiles.size() + "}";
    }
}
