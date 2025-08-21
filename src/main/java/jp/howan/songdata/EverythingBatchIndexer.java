package jp.howan.songdata;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * EverythingBatchIndexer: bmsroot 配列を基に Everything を一括で叩き、
 * フォルダ単位の FolderInfo マップ (Map<Path,FolderInfo>) を作るユーティリティ。
 * 動作前提:
 * - EverythingDirect.isAvailable() が true のときに EverythingDirect のネイティブがロードされていること
 * - bmsRoots に指定した文字列配列はパス（絶対 or 相対）として扱えること
 * - ルート群は相対パスであっても検索時は絶対パスに変換する
 */
public final class EverythingBatchIndexer {
    private final String[] bmsRoots;

    public EverythingBatchIndexer(String[] bmsRoots) {
        this.bmsRoots = bmsRoots != null ? bmsRoots.clone() : new String[0];
    }

    /**
     * ルート群以下の BMS/txt/preview を一括で集め、BMS が存在するフォルダのみをキーとする Map<Path,FolderInfo> を返す。
     */
    public Map<Path, FolderInfo> buildFolderInfoMap() {
        if (!EverythingDirect.isAvailable()) return Collections.emptyMap();
        String rootClause = buildRootClause(); // file:<"r1\"|"r2\">
        if (rootClause.isEmpty()) return Collections.emptyMap();

        // 1) BMS 一括取得（全サブディレクトリを含む）
        String bmsSearch = rootClause + " ext:bms;bme;bml;pms;bmson";
        List<Path> allBms = EverythingDirect.doSearchCollect(bmsSearch);

        // Map を作る（キー = 親フォルダ）
        Map<Path, FolderInfo> map = new HashMap<>(Math.max(16, allBms.size() / 4));
        for (Path p : allBms) {
            Path parent = p.getParent();
            if (parent == null) continue;
            FolderInfo fi = map.computeIfAbsent(parent, k -> new FolderInfo());
            fi.bmsFiles.add(p);
        }

        if (map.isEmpty()) {
            // BMS を含むフォルダが0なら続ける必要はない（preview/txt は不要）
            return map;
        }

        // 2) txt 一括取得 -> parent が map にある場合だけフラグを立てる
        String txtSearch = rootClause + " ext:txt";
        List<Path> allTxt = EverythingDirect.doSearchCollect(txtSearch);
        for (Path p : allTxt) {
            Path parent = p.getParent();
            if (parent == null) continue;
            FolderInfo fi = map.get(parent);
            if (fi != null) {
                fi.hasTxt = true;
            }
        }

        // 3) preview 一括取得 -> parent が map にある場合だけ preview を追加する
        String previewSearch = rootClause + " startwith:preview ext:wav;ogg;mp3;flac";
        List<Path> allPreview = EverythingDirect.doSearchCollect(previewSearch);
        for (Path p : allPreview) {
            Path parent = p.getParent();
            if (parent == null) continue;
            FolderInfo fi = map.get(parent);
            if (fi != null) {
                fi.previewFiles.add(p);
            }
        }

        return map;
    }

    // ----- helper to build file:<...> clause using <> group and | as OR -----
    private String buildRootClause() {
        if (bmsRoots == null || bmsRoots.length == 0) return "";
        String joined = Arrays.stream(bmsRoots)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(path -> Path.of(path).toAbsolutePath().toString()) // 相対パスではなく絶対パスで検索を行う
                .map(this::quoteAndEnsureTrailingSlash)
                .collect(Collectors.joining("|"));
        return "file:<" + joined + ">";
    }

    private String quoteAndEnsureTrailingSlash(String rawPath) {
        String p = rawPath.replace("\"", "");
        if (!p.endsWith("\\") && !p.endsWith("/")) p = p + "\\";
        return "\"" + p + "\"";
    }
}
