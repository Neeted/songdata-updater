package jp.howan.songdata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static jp.howan.songdata.EverythingFacade.doSearchCollectWithDates;

/**
 * EverythingBatchIndexer: bmsroot 配列を基に Everything を一括で叩き、
 * フォルダ単位の FolderInfo マップ (Map<Path,FolderInfo>) を作るユーティリティ。
 * 動作前提:
 * - EverythingDirect.isAvailable() が true のときに EverythingDirect のネイティブがロードされていること
 * - bmsRoots に指定した文字列配列はパス（絶対 or 相対）として扱えること
 * - ルート群は相対パスであっても検索時は絶対パスに変換する
 */
public final class EverythingBatchIndexer {
    private final String[] bmsRoots; // コンフィグ記載の元のパス
    List<Path> absBmsRoots = new ArrayList<>(); // 元が絶対パスのbmsroot
    List<Path> relBmsRoots = new ArrayList<>(); // 元が相対パスのbmsroot(絶対パスに変換後格納)
    Path cd = Path.of("").toAbsolutePath();


    public EverythingBatchIndexer(String[] bmsRoots) {
        this.bmsRoots = bmsRoots != null ? bmsRoots.clone() : new String[0];
        for (String root : this.bmsRoots) {
            Path p = Paths.get(root);
            Path absPath = p.toAbsolutePath().normalize();
            if (p.isAbsolute()) {
                absBmsRoots.add(absPath);
            } else {
                relBmsRoots.add(absPath);
            }
        }
    }

    /**
     * ルートパス群（bmsRoots）以下の BMS 、txt 、preview を一括で取得し、
     * BMS が存在するフォルダごとの情報をまとめたマップを構築します。
     * <p>処理の流れ:</p>
     * <ol>
     *   <li>BMS / txt / preview を Everything で検索（絶対パス・相対パスを区別して取得）</li>
     *   <li>検索結果からファイルが存在するフォルダをキーとする Map<Path, BmsFolder> を作成</li>
     *   <li>結果リストをforで回し拡張子ごとにBmsFolderに追加する</li>
     * </ol>
     * 相対パスの BMS ルートに対しては、検索結果のパスをカレントディレクトリ基準で相対化して格納します。
     * @return bmsFolderMap
     */
    public Map<Path, BmsFolder> buildBmsFolderMap() {
        if (bmsRoots.length == 0) return Collections.emptyMap();

        // 1) BMS 一括取得（全サブディレクトリを含む）
        List<EverythingSearchResult> allAbsFile = new ArrayList<>();
        List<EverythingSearchResult> allRelFile = new ArrayList<>();
        if (!absBmsRoots.isEmpty()) {
            String absSearch = "file:<" + joinPaths(absBmsRoots) + "> <<ext:bms;bme;bml;pms;bmson;txt>|<startwith:preview ext:wav;ogg;mp3;flac>>";
            allAbsFile = doSearchCollectWithDates(absSearch);
        }
        if (!relBmsRoots.isEmpty()) {
            String relSearch = "file:<" + joinPaths(relBmsRoots) + "> <<ext:bms;bme;bml;pms;bmson;txt>|<startwith:preview ext:wav;ogg;mp3;flac>>";
            allRelFile = doSearchCollectWithDates(relSearch);
        }

        // bmsFolderMap を作る（キー = 親フォルダ）
        Map<Path, BmsFolder> bmsFolderMap = new HashMap<>(Math.max(16, (allAbsFile.size() + allRelFile.size()) / 4));
        for (EverythingSearchResult sr : allAbsFile) {
            Path parent = sr.path().getParent();
            if (parent == null) continue;
            BmsFolder bf = bmsFolderMap.computeIfAbsent(parent, k -> new BmsFolder());
            String s = sr.path().getFileName().toString().toLowerCase();
            if (s.endsWith(".bms") || s.endsWith(".bme") || s.endsWith(".bml") || s.endsWith(".pms") || s.endsWith(".bmson")) {
                bf.bmsFiles.add(sr);
            }
            if (s.endsWith(".txt")) bf.hasTxt = true;
            if (s.startsWith("preview") && (s.endsWith(".wav") || s.endsWith(".ogg") || s.endsWith(".mp3") || s.endsWith(".flac"))) {
                bf.previewFiles.add(sr.path());
            }
        }
        for (EverythingSearchResult absSr : allRelFile) {
            // 相対パスbmsrootの場合、検索結果のBMSフォルダパスやBMSファイルパスを相対パスに変換してbmsFolderMapに格納する
            EverythingSearchResult relSr = new EverythingSearchResult(cd.relativize(absSr.path().normalize()), absSr.lastModified());
            Path parent = relSr.path().getParent();
            if (parent == null) continue;
            BmsFolder bf = bmsFolderMap.computeIfAbsent(parent, k -> new BmsFolder());
            String s = relSr.path().getFileName().toString().toLowerCase();
            if (s.endsWith(".bms") || s.endsWith(".bme") || s.endsWith(".bml") || s.endsWith(".pms") || s.endsWith(".bmson")) {
                bf.bmsFiles.add(relSr);
            }
            if (s.endsWith(".txt")) bf.hasTxt = true;
            if (s.startsWith("preview") && (s.endsWith(".wav") || s.endsWith(".ogg") || s.endsWith(".mp3") || s.endsWith(".flac"))) {
                bf.previewFiles.add(relSr.path());
            }
        }

        return bmsFolderMap;
    }

    public NavigableMap<Path, Long> buildScanFolders() throws IOException {
        NavigableMap<Path, Long> scanFolders = new TreeMap<>();

        // 検索結果にルートフォルダ自体は含まれないのでここで追加
        for (String s : bmsRoots) {
            Path p = Paths.get(s);
            scanFolders.put(p, Files.getLastModifiedTime(p).toMillis() / 1000);
        }

        // 絶対/相対パスごとに検索しscanFoldersに格納
        if (!absBmsRoots.isEmpty()) {
            List<EverythingSearchResult> allAbsFolder;
            String allAbsFolderSearch = "folder:<" + joinPaths(absBmsRoots) + ">";
            allAbsFolder = doSearchCollectWithDates(allAbsFolderSearch);
            for (EverythingSearchResult sr : allAbsFolder) {
                scanFolders.put(sr.path(), sr.lastModified());
            }
        }
        if (!relBmsRoots.isEmpty()) {
            List<EverythingSearchResult> allRelFolder;
            String allRelFolderSearch = "folder:<" + joinPaths(relBmsRoots) + ">";
            allRelFolder = doSearchCollectWithDates(allRelFolderSearch);
            for (EverythingSearchResult sr : allRelFolder) {
                scanFolders.put(cd.relativize(sr.path()), sr.lastModified());
            }
        }
        return scanFolders;
    }

    /**
     * 絶対パスのListをEverythingのOR検索用のクエリに整形する
     * @param paths 絶対パスのList
     * @return Everything用の検索クエリ
     */
    private String joinPaths(List<Path> paths) {
        // 各Pathをダブルクオートで囲み、|で連結
        String joined = paths.stream()
                .map(p -> quoteAndEnsureTrailingSlash(p.toString()))
                .collect(Collectors.joining("|"));
        // 全体を<>で囲む
        return "<" + joined + ">";
    }

    /**
     * Everything検索用にディレクトリパスを整形する
     * "D:\BMS"のようなパスを"D:\BMS\"にする、末尾に区切り文字がないと正確にディレクトリ配下の検索ができない
     * folder:検索を行う場合、検索パス自体は結果に含まれないということに注意
     * EverythingはWindows専用なので、File.separatorは使わない
     * @param rawPath 成形前のパス文字列、PathオブジェクトをtoString()したような形式を想定
     * @return 整形後のパス文字列
     */
    private String quoteAndEnsureTrailingSlash(String rawPath) {
        String p = rawPath.replace("\"", "");
        if (!p.endsWith("\\")) p = p + "\\";
        return "\"" + p + "\"";
    }
}
