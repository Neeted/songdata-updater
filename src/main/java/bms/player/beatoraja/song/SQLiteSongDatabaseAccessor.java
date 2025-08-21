package bms.player.beatoraja.song;

import jp.howan.songdata.EverythingDirect;
import jp.howan.songdata.EverythingBatchIndexer;
import jp.howan.songdata.FolderInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import bms.player.beatoraja.SQLiteDatabaseAccessor;
import bms.player.beatoraja.Validatable;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.SQLiteDataSource;

import bms.model.*;

/**
 * 楽曲データベースへのアクセスクラス
 * 
 * @author exch
 */
public class SQLiteSongDatabaseAccessor extends SQLiteDatabaseAccessor implements SongDatabaseAccessor {

	private SQLiteDataSource ds;

	private final Path root;

	private final ResultSetHandler<List<SongData>> songhandler = new BeanListHandler<SongData>(SongData.class);
	private final ResultSetHandler<List<FolderData>> folderhandler = new BeanListHandler<FolderData>(FolderData.class);

	private final QueryRunner qr;
	
	private List<SongDatabaseAccessorPlugin> plugins = new ArrayList();
	
	public SQLiteSongDatabaseAccessor(String filepath, String[] bmsroot) throws ClassNotFoundException {
		super(new Table("folder", 
				new Column("title", "TEXT"),
				new Column("subtitle", "TEXT"),
				new Column("command", "TEXT"),
				new Column("path", "TEXT", 0, 1),
				new Column("banner", "TEXT"),
				new Column("parent", "TEXT"),
				new Column("type", "INTEGER"),
				new Column("date", "INTEGER"),
				new Column("adddate", "INTEGER"),
				new Column("max", "INTEGER")
				),
				new Table("song",
						new Column("md5", "TEXT", 1, 0),
						new Column("sha256", "TEXT", 1, 0),
						new Column("title", "TEXT"),
						new Column("subtitle", "TEXT"),
						new Column("genre", "TEXT"),
						new Column("artist", "TEXT"),
						new Column("subartist", "TEXT"),
						new Column("tag", "TEXT"),
						new Column("path", "TEXT", 0, 1),
						new Column("folder", "TEXT"),
						new Column("stagefile", "TEXT"),
						new Column("banner", "TEXT"),
						new Column("backbmp", "TEXT"),
						new Column("preview", "TEXT"),
						new Column("parent", "TEXT"),
						new Column("level", "INTEGER"),
						new Column("difficulty", "INTEGER"),
						new Column("maxbpm", "INTEGER"),
						new Column("minbpm", "INTEGER"),
						new Column("length", "INTEGER"),
						new Column("mode", "INTEGER"),
						new Column("judge", "INTEGER"),
						new Column("feature", "INTEGER"),
						new Column("content", "INTEGER"),
						new Column("date", "INTEGER"),
						new Column("favorite", "INTEGER"),
						new Column("adddate", "INTEGER"),
						new Column("notes", "INTEGER"),
						new Column("charthash", "TEXT")
						));
		
		Class.forName("org.sqlite.JDBC");
		SQLiteConfig conf = new SQLiteConfig();
		conf.setSharedCache(true);
		conf.setSynchronous(SynchronousMode.OFF);
		// conf.setJournalMode(JournalMode.MEMORY);
		ds = new SQLiteDataSource(conf);
		ds.setUrl("jdbc:sqlite:" + filepath);
		qr = new QueryRunner(ds);
		root = Paths.get(".");
		createTable();
	}
		
	public void addPlugin(SongDatabaseAccessorPlugin plugin) {
		plugins.add(plugin);
	}
	
	/**
	 * 楽曲データベースを初期テーブルを作成する。 すでに初期テーブルを作成している場合は何もしない。
	 */
	private void createTable() {
		try {
			// songテーブル作成(存在しない場合)
			validate(qr);
			
			if(qr.query("PRAGMA TABLE_INFO(song)", new MapListHandler()).stream().anyMatch(m -> m.get("name").equals("sha256") && (int)(m.get("pk")) == 1)) {
				qr.update("ALTER TABLE [song] RENAME TO [old_song]");
				validate(qr);
				qr.update("INSERT INTO song SELECT "
						+ "md5, sha256, title, subtitle, genre, artist, subartist, tag, path,"
						+ "folder, stagefile, banner, backbmp, preview, parent, level, difficulty,"
						+ "maxbpm, minbpm, length, mode, judge, feature, content,"
						+ "date, favorite, notes, adddate, charthash "
						+ "FROM old_song GROUP BY path HAVING MAX(adddate)");
				qr.update("DROP TABLE old_song");
			}
		} catch (SQLException e) {
			Logger.getGlobal().severe("楽曲データベース初期化中の例外:" + e.getMessage());
		}
	}

	
	/**
	 * 楽曲を取得する
	 * 
	 * @param key
	 *            属性
	 * @param value
	 *            属性値
	 * @return 検索結果
	 */
	public SongData[] getSongDatas(String key, String value) {
		try {
			final List<SongData> m = qr.query("SELECT * FROM song WHERE " + key + " = ?", songhandler, value);
			return Validatable.removeInvalidElements(m).toArray(new SongData[m.size()]);
		} catch (Exception e) {
			e.printStackTrace();
			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
		}
		return SongData.EMPTY;
	}

	/**
	 * MD5/SHA256で指定した楽曲をまとめて取得する
	 * 
	 * @param hashes
	 *            楽曲のMD5/SHA256
	 * @return 取得した楽曲
	 */
	public SongData[] getSongDatas(String[] hashes) {
		try {
			StringBuilder md5str = new StringBuilder();
			StringBuilder sha256str = new StringBuilder();
			for (String hash : hashes) {
				if (hash.length() > 32) {
					if (sha256str.length() > 0) {
						sha256str.append(',');
					}
					sha256str.append('\'').append(hash).append('\'');
				} else {
					if (md5str.length() > 0) {
						md5str.append(',');
					}
					md5str.append('\'').append(hash).append('\'');
				}
			}
			List<SongData> m = qr.query("SELECT * FROM song WHERE md5 IN (" + md5str.toString() + ") OR sha256 IN ("
					+ sha256str.toString() + ")", songhandler);
			
			// 検索並び順保持
			List<SongData> sorted = m.stream().sorted((a, b) -> {
				int aIndexSha256 = -1,aIndexMd5 = -1,bIndexSha256 = -1,bIndexMd5 = -1;
				for(int i = 0;i < hashes.length;i++) {
					if(hashes[i].equals(a.getSha256())) aIndexSha256 = i;
					if(hashes[i].equals(a.getMd5())) aIndexMd5 = i;
					if(hashes[i].equals(b.getSha256())) bIndexSha256 = i;
					if(hashes[i].equals(b.getMd5())) bIndexMd5 = i;
				}
			    int aIndex = Math.min((aIndexSha256 == -1 ? Integer.MAX_VALUE : aIndexSha256), (aIndexMd5 == -1 ? Integer.MAX_VALUE : aIndexMd5));
			    int bIndex = Math.min((bIndexSha256 == -1 ? Integer.MAX_VALUE : bIndexSha256), (bIndexMd5 == -1 ? Integer.MAX_VALUE : bIndexMd5));
			    return bIndex - aIndex;
            }).collect(Collectors.toList());

			SongData[] validated = Validatable.removeInvalidElements(sorted).toArray(new SongData[m.size()]);
			return validated;
		} catch (Exception e) {
			e.printStackTrace();
			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
		}

		return SongData.EMPTY;
	}

	public SongData[] getSongDatas(String sql, String score, String scorelog, String info) {
		try (Statement stmt = qr.getDataSource().getConnection().createStatement()) {
			stmt.execute("ATTACH DATABASE '" + score + "' as scoredb");
			stmt.execute("ATTACH DATABASE '" + scorelog + "' as scorelogdb");
			List<SongData> m;

			if(info != null) {
				stmt.execute("ATTACH DATABASE '" + info + "' as infodb");
				String s = "SELECT DISTINCT md5, song.sha256 AS sha256, title, subtitle, genre, artist, subartist,path,folder,stagefile,banner,backbmp,parent,level,difficulty,"
						+ "maxbpm,minbpm,song.mode AS mode, judge, feature, content, song.date AS date, favorite, song.notes AS notes, adddate, preview, length, charthash"
						+ " FROM song INNER JOIN (information LEFT OUTER JOIN (score LEFT OUTER JOIN scorelog ON score.sha256 = scorelog.sha256) ON information.sha256 = score.sha256) "
						+ "ON song.sha256 = information.sha256 WHERE " + sql;
				ResultSet rs = stmt.executeQuery(s);
				m = songhandler.handle(rs);
//				System.out.println(s + " -> result : " + m.size());
				stmt.execute("DETACH DATABASE infodb");
			} else {
				String s = "SELECT DISTINCT md5, song.sha256 AS sha256, title, subtitle, genre, artist, subartist,path,folder,stagefile,banner,backbmp,parent,level,difficulty,"
						+ "maxbpm,minbpm,song.mode AS mode, judge, feature, content, song.date AS date, favorite, song.notes AS notes, adddate, preview, length, charthash"
						+ " FROM song LEFT OUTER JOIN (score LEFT OUTER JOIN scorelog ON score.sha256 = scorelog.sha256) ON song.sha256 = score.sha256 WHERE " + sql;
				ResultSet rs = stmt.executeQuery(s);
				m = songhandler.handle(rs);
			}
			stmt.execute("DETACH DATABASE scorelogdb");				
			stmt.execute("DETACH DATABASE scoredb");
			return Validatable.removeInvalidElements(m).toArray(new SongData[m.size()]);
		} catch(Throwable e) {
			e.printStackTrace();			
		}

		return SongData.EMPTY;

	}

	public SongData[] getSongDatasByText(String text) {
		try {
			List<SongData> m = qr.query(
					"SELECT * FROM song WHERE rtrim(title||' '||subtitle||' '||artist||' '||subartist||' '||genre) LIKE ?"
							+ " GROUP BY sha256",songhandler, "%" + text + "%");
			return Validatable.removeInvalidElements(m).toArray(new SongData[m.size()]);
		} catch (Exception e) {
			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
		}

		return SongData.EMPTY;
	}
	
	/**
	 * 楽曲を取得する
	 * 
	 * @param key
	 *            属性
	 * @param value
	 *            属性値
	 * @return 検索結果
	 */
	public FolderData[] getFolderDatas(String key, String value) {
		try {
			final List<FolderData> m = qr.query("SELECT * FROM folder WHERE " + key + " = ?", folderhandler, value);
			return m.toArray(new FolderData[m.size()]);
		} catch (Exception e) {
			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
		}

		return FolderData.EMPTY;
	}

	/**
	 * 楽曲を更新する
	 * 
	 * @param songs 更新する楽曲
	 */
	public void setSongDatas(SongData[] songs) {
		try (Connection conn = qr.getDataSource().getConnection()){
			conn.setAutoCommit(false);

			for (SongData sd : songs) {
				this.insert(qr, conn, "song", sd);
			}
			conn.commit();
			conn.close();
		} catch (Exception e) {
			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
		}
	}

	/**
	 * データベースを更新する
	 * 
	 * @param path
	 *            LR2のルートパス
	 */
	public void updateSongDatas(String path, String[] bmsroot, boolean updateAll, SongInformationAccessor info) {
		if(bmsroot == null || bmsroot.length == 0) {
			Logger.getGlobal().warning("楽曲ルートフォルダが登録されていません");
			return;
		}
		SongDatabaseUpdater updater = new SongDatabaseUpdater(updateAll, bmsroot, info);
		updater.updateSongDatas(path == null ? Stream.of(bmsroot).map(p -> Paths.get(p)) : Stream.of(Paths.get(path)));
	}
	
	/**
	 * song database更新用クラス
	 * 
	 * @author exch
	 */
	class SongDatabaseUpdater {

		private final boolean updateAll;
		private final String[] bmsroot;

		private SongInformationAccessor info;

		public SongDatabaseUpdater(boolean updateAll, String[] bmsroot, SongInformationAccessor info) {
			this.updateAll = updateAll;
			this.bmsroot = bmsroot;
			this.info = info;
		}

		/**
		 * データベースを更新する
		 * 
		 * @param paths
		 *            更新するディレクトリ(ルートディレクトリでなくても可)
		 */
		public void updateSongDatas(Stream<Path> paths) {
            // TODO songinfo.db用処理のバッチ化といった高速化、現状オリジナル処理のまま、走査、デコード、DB処理で別スレッド化という案もあり
			long time = System.currentTimeMillis();
			SongDatabaseUpdaterProperty property = new SongDatabaseUpdaterProperty(Calendar.getInstance().getTimeInMillis() / 1000, info);
			property.count.set(0);

			if(info != null) {
                // songinfo.db用トランザクション開始
				info.startUpdate();
			}

            // 新処理用DB操作件数カウンタ
            final java.util.concurrent.atomic.AtomicInteger songInsertCount = new java.util.concurrent.atomic.AtomicInteger(0);
            final java.util.concurrent.atomic.AtomicInteger songUpdateCount = new java.util.concurrent.atomic.AtomicInteger(0);
            final java.util.concurrent.atomic.AtomicInteger songDeleteCount = new java.util.concurrent.atomic.AtomicInteger(0);
            final java.util.concurrent.atomic.AtomicInteger folderInsertCount = new java.util.concurrent.atomic.AtomicInteger(0);
            final java.util.concurrent.atomic.AtomicInteger folderDeleteCount = new java.util.concurrent.atomic.AtomicInteger(0);

            // songdata.db用トランザクション開始
			try (Connection conn = ds.getConnection()) {
				property.conn = conn;
				conn.setAutoCommit(false);
				// 楽曲のタグ,FAVORITEの保持
				for (SongData record : qr.query(conn, "SELECT sha256, tag, favorite FROM song", songhandler)) {
					if (record.getTag().length() > 0) {
						property.tags.put(record.getSha256(), record.getTag());
					}
					if (record.getFavorite() > 0) {
						property.favorites.put(record.getSha256(), record.getFavorite());
					}
				}
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの保持が完了しました");
				if(updateAll) {
					qr.update(conn, "DELETE FROM folder");					
					qr.update(conn, "DELETE FROM song");
				} else {
					// ルートディレクトリに含まれないフォルダの削除
					StringBuilder dsql = new StringBuilder();
					Object[] param = new String[bmsroot.length];
					for (int i = 0; i < bmsroot.length; i++) {
						dsql.append("path NOT LIKE ?");
						param[i] = bmsroot[i] + "%";
						if (i < bmsroot.length - 1) {
							dsql.append(" AND ");
						}
					}
					qr.update(conn,
							"DELETE FROM folder WHERE path NOT LIKE 'LR2files%' AND path NOT LIKE '%.lr2folder' AND "
									+ dsql.toString(), param);
					qr.update(conn, "DELETE FROM song WHERE " + dsql.toString(), param);					
				}

                // 以下新処理
                final int BATCH_SIZE = 1000; // バッチサイズ（必要に応じて調整）

                // song/folder を全件読み込み、メモリ上の Map にする（参照用）
                final NavigableMap<String, SongData> songMap = new TreeMap<>();
                final List<SongData> allSongs = qr.query(conn, "SELECT path, date, preview, sha256 FROM song", songhandler);
                for (SongData s : allSongs) {
                    if (s != null && s.getPath() != null) songMap.put(s.getPath(), s);
                }

                final Map<String, FolderData> folderMap = new HashMap<>();
                final List<FolderData> allFolders = qr.query(conn, "SELECT path, date, parent FROM folder", folderhandler);
                for (FolderData f : allFolders) {
                    if (f != null && f.getPath() != null) folderMap.put(f.getPath(), f);
                }

                Logger.getGlobal().info("song/folderテーブルのハッシュマップ構築が完了しました");

                // dir の mtime キャッシュ（pre で入れ post で参照）
                final Map<String, Long> dirMtimeCache = new HashMap<>();

                // SQL 文（テーブル定義の列順に合わせること）
                final String songInsertSQL = "INSERT OR REPLACE INTO song (md5, sha256, title, subtitle, genre, artist, subartist, tag, path, folder, stagefile, banner, backbmp, preview, parent, level, difficulty, maxbpm, minbpm, length, mode, judge, feature, content, date, favorite, adddate, notes, charthash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
                final String folderInsertSQL = "INSERT OR REPLACE INTO folder (title, subtitle, command, path, banner, parent, type, date, adddate, max) VALUES (?,?,?,?,?,?,?,?,?,?)";

                // PreparedStatement を作って再利用する（ラムダ内から参照するため final）try-with-resources psSong/psFolder
                try (final PreparedStatement psSong = conn.prepareStatement(songInsertSQL);
                     final PreparedStatement psFolder = conn.prepareStatement(folderInsertSQL)) {

                    // バッチのペンディング件数（ラムダから変更できるよう AtomicInteger を使う）
                    final java.util.concurrent.atomic.AtomicInteger pendingSong = new java.util.concurrent.atomic.AtomicInteger(0);
                    final java.util.concurrent.atomic.AtomicInteger pendingFolder = new java.util.concurrent.atomic.AtomicInteger(0);

                    // 小ヘルパ: song バッチ flush（PreparedStatement を executeBatch）
                    final Runnable flushSong = () -> {
                        int toFlush = pendingSong.get();
                        if (toFlush <= 0) return;
                        try {
                            // 実行
                            int[] results = psSong.executeBatch();
                            // 成功件数のカウント（戻り値の解釈はドライバ依存なので、pending をそのまま使う）
                            songInsertCount.addAndGet(toFlush);
                        } catch (SQLException e) {
                            Logger.getGlobal().severe("song バッチ挿入中の例外: " + e.getMessage());
                            try {
                                conn.rollback();
                            } catch (SQLException ex) {
                                Logger.getGlobal().severe("rollback エラー: " + ex.getMessage());
                            }
                        } finally {
                            // PreparedStatement の batch は自動的にクリアされる実装が多いが明示的にクリア
                            try { psSong.clearBatch(); } catch (SQLException ignore) {}
                            pendingSong.set(0);
                        }
                    };

                    // 小ヘルパ: folder バッチ flush
                    final Runnable flushFolder = () -> {
                        int toFlush = pendingFolder.get();
                        if (toFlush <= 0) return;
                        try {
                            int[] results = psFolder.executeBatch();
                            folderInsertCount.addAndGet(toFlush);
                        } catch (SQLException e) {
                            Logger.getGlobal().severe("folder バッチ挿入中の例外: " + e.getMessage());
                            try {
                                conn.rollback();
                            } catch (SQLException ex) {
                                Logger.getGlobal().severe("rollback エラー: " + ex.getMessage());
                            }
                        } finally {
                            try { psFolder.clearBatch(); } catch (SQLException ignore) {}
                            pendingFolder.set(0);
                        }
                    };

                    // Everything SDK利用のためのラッパーインスタンスを作る
                    Map<Path, FolderInfo> folderInfoMap;
                    if (EverythingDirect.isAvailable()) {
                        // Everythingが利用できる場合、BMSが存在するフォルダの情報(BMSパスリスト、txt有無のフラグ、preview音源パスリスト)をフォルダパスをキーとしたハッシュマップで構築する
                        Logger.getGlobal().info("Everythingをファイル検索に利用します(Everythingのインデックスは最新化されている前提です)");
                        EverythingBatchIndexer indexer = new EverythingBatchIndexer(this.bmsroot);
                        folderInfoMap = indexer.buildFolderInfoMap();
                        Logger.getGlobal().info("EverythingによるBMSフォルダ情報ハッシュマップの構築が完了しました");
                    } else {
                        Logger.getGlobal().info("Everythingが利用できないので通常のディレクトリ走査を行います");
                        folderInfoMap = Collections.emptyMap();
                    }

                    // ルートごとにツリーを走査（preVisitDirectory で完結）
                    for (Iterator<Path> it = paths.iterator(); it.hasNext();) {
                        final Path scanRoot = it.next();
                        Logger.getGlobal().info("走査中ルート: " + scanRoot.toString());

                        Files.walkFileTree(scanRoot, new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                                final String dirKey = (dir.startsWith(root) ? root.relativize(dir).toString() : dir.toString()) + File.separatorChar;
                                final FolderData folderRecord = folderMap.get(dirKey);
                                final long dirMtime = attrs.lastModifiedTime().toMillis() / 1000;
                                dirMtimeCache.put(dirKey, dirMtime);

                                // ディレクトリ直下ファイル確認(途中ディレクトリかBMSフォルダか判定、preview音源やtxtがあるか)
                                List<Path> bmsFiles = new ArrayList<>();
                                boolean txtPresent = false;
                                String previewpath = null;
                                boolean isUpdateDir = folderRecord == null || folderRecord.getDate() != dirMtime;
                                if (EverythingDirect.isAvailable()) {
                                    // Everythingが使える場合は事前構築したハッシュマップで判定を行える
                                    if (scanRoot.isAbsolute()) {
                                        // 走査中のルートが絶対パスの場合は、Everythingの結果を素直に扱える
                                        if (isUpdateDir) {
                                            // 更新ありフォルダ
                                            FolderInfo fi = folderInfoMap.get(dir);
                                            bmsFiles = fi != null ? fi.bmsFiles : Collections.emptyList();
                                            txtPresent = fi != null && fi.hasTxt;
                                            List<Path> previews = fi != null ? fi.previewFiles : Collections.emptyList();
                                            previewpath = previews.isEmpty() ? null : previews.get(0).getFileName().toString();
                                        } else {
                                            // 更新なしフォルダ
                                            FolderInfo fi = folderInfoMap.get(dir);
                                            bmsFiles = fi != null ? fi.bmsFiles : Collections.emptyList();
                                        }
                                    } else {
                                        // 走査中のルートが相対パスの場合は、Everythingの結果を相対パスに変換しないと、DBのpathの値と不整合が生じて正常に処理できない
                                        Path cd = Path.of("").toAbsolutePath(); // カレントディレクトリ
                                        Path absDir = dir.toAbsolutePath();
                                        if (isUpdateDir) {
                                            // 更新ありフォルダ
                                            FolderInfo fi = folderInfoMap.get(absDir);
                                            bmsFiles = fi != null
                                                    ? fi.bmsFiles.stream()
                                                        .map(path -> cd.relativize(path.normalize()))
                                                        .toList()
                                                    : Collections.emptyList();
                                            txtPresent = fi != null && fi.hasTxt;
                                            List<Path> previews = fi != null ? fi.previewFiles : Collections.emptyList();
                                            previewpath = previews.isEmpty() ? null : previews.get(0).getFileName().toString();
                                        } else {
                                            // 更新なしフォルダ
                                            FolderInfo fi = folderInfoMap.get(absDir);
                                            bmsFiles = fi != null
                                                    ? fi.bmsFiles.stream()
                                                    .map(path -> cd.relativize(path.normalize()))
                                                    .toList()
                                                    : Collections.emptyList();
                                        }
                                    }
                                } else {
                                    // Everythingが使えない場合は従来の DirectoryStream ベース処理（フォールバック）
                                    // 更新ありフォルダは全列挙、更新なしフォルダは途中ディレクトリかどうかの判定が出来ればよいのでBMS1個で止める
                                    try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
                                        if (isUpdateDir) {
                                            // 更新ありフォルダ
                                            for (Path p : ds) {
                                                if (Files.isDirectory(p)) continue;
                                                final String name = p.getFileName().toString();
                                                final String lname = name.toLowerCase();
                                                if (lname.endsWith(".bms") || lname.endsWith(".bme") || lname.endsWith(".bml") || lname.endsWith(".pms") || lname.endsWith(".bmson")) {
                                                    bmsFiles.add(p);
                                                } else {
                                                    if (previewpath == null) {
                                                        if (lname.startsWith("preview") && (lname.endsWith(".wav") || lname.endsWith(".ogg") || lname.endsWith(".mp3") || lname.endsWith(".flac"))) {
                                                            previewpath = name;
                                                        }
                                                    }
                                                    if (!txtPresent && lname.endsWith(".txt")) txtPresent = true;
                                                }
                                            }
                                        } else {
                                            // 更新なしフォルダ
                                            for (Path p : ds) {
                                                if (Files.isDirectory(p)) continue;
                                                final String name = p.getFileName().toString();
                                                final String lname = name.toLowerCase();
                                                if (lname.endsWith(".bms") || lname.endsWith(".bme") || lname.endsWith(".bml") || lname.endsWith(".pms") || lname.endsWith(".bmson")) {
                                                    bmsFiles.add(p);
                                                }
                                                if (!bmsFiles.isEmpty()) break;
                                            }
                                        }
                                    } catch (IOException e) {
                                        // 無視して続行（従来挙動に合わせる）
                                    }
                                }

                                // 直下 BMS がない途中ディレクトリの場合は通常走査（postVisitDirectory で処理）
                                if (bmsFiles.isEmpty()) return FileVisitResult.CONTINUE;

                                // 直下に BMS があり、且つ日付一致ならサブツリーをスキップ(更新日時が違う場合はpreview音源が追加されている可能性あり)
                                if (!isUpdateDir) {
                                    folderMap.remove(dirKey); // フォルダを処理済みにする
                                    // 更新なしフォルダ以下のBMSを処理済みにする
                                    NavigableMap<String, SongData> tail = songMap.tailMap(dirKey, true);
                                    Iterator<String> tit = tail.keySet().iterator();
                                    while (tit.hasNext()) {
                                        String key = tit.next();
                                        if (!key.startsWith(dirKey)) break;
                                        tit.remove();
                                    }
                                    return FileVisitResult.SKIP_SUBTREE;
                                }

                                // 更新ありケース: 各 BMS を処理
                                for (Path bmsPath : bmsFiles) {
                                    final String pathname = (bmsPath.startsWith(root) ? root.relativize(bmsPath).toString() : bmsPath.toString());
                                    long lastModified = -1;
                                    try { lastModified = Files.getLastModifiedTime(bmsPath).toMillis() / 1000; } catch (IOException e) {}

                                    // 対象BMSを処理済みにして、songのDELETE対象から外す
                                    final SongData existing = songMap.remove(pathname);
                                    // 既存BMS(songテーブルのレコードとフルパス名と更新日時が一致)の場合はpreview音源のみ更新処理する
                                    if (existing != null && existing.getDate() == lastModified) {
                                        final String oldpp = existing.getPreview() == null ? "" : existing.getPreview();
                                        final String newpp = previewpath == null ? "" : previewpath;
                                        // DBのpreviewとフォルダ内のpreviewが一致していない、かつ、フォルダ内のpreviewが空じゃない場合は、フォルダ内のpreviewをsongにUPDATEする
                                        // 単に一致しない場合新しいものにすると、#PREVIEW _preview.wavのように指定されていた時に、音源が消えてしまうことになるので実在する場合のみ更新
                                        if (!oldpp.equals(newpp) && !newpp.isEmpty()) {
                                            try {
                                                int updated = qr.update(property.conn, "UPDATE song SET preview=? WHERE path = ?", newpp, pathname);
                                                if (updated > 0) songUpdateCount.addAndGet(updated);
                                            } catch (SQLException e) {
                                                Logger.getGlobal().warning("Error while updating preview at " + pathname + ": " + e.getMessage());
                                            }
                                        }
                                        continue; // 既存BMSのpreview音源のみ更新してcontinue
                                    }

                                    // 新規BMSなのでデコード処理に進む
                                    BMSModel model = null;
                                    try {
                                        if (pathname.toLowerCase().endsWith(".bmson")) {
                                            BMSONDecoder decoder = new BMSONDecoder(BMSModel.LNTYPE_LONGNOTE);
                                            model = decoder.decode(bmsPath);
                                        } else {
                                            BMSDecoder decoder = new BMSDecoder(BMSModel.LNTYPE_LONGNOTE);
                                            model = decoder.decode(bmsPath);
                                        }
                                    } catch (Exception e) {
                                        Logger.getGlobal().severe("Error while decoding " + pathname + ": " + e.getMessage());
                                    }
                                    if (model == null) continue; // デコードできなかったらcontinue

                                    SongData sd = null;
                                    try {
                                        sd = new SongData(model, txtPresent);
                                    } catch (Throwable t) {
                                        Logger.getGlobal().severe("SongData 生成失敗 : path=" + pathname + " cause=" + t.getMessage());
                                        t.printStackTrace();
                                        continue; // BMSをmodelにデコードできたのに、songdataにできなかった場合もcontinue
                                    }

                                    // 0ノーツではない、または、WAV定義が0ではない、場合はsongdataの挿入へ進む
                                    if (sd.getNotes() != 0 || model.getWavList().length != 0) {
                                        // 難易度未設定時の自動設定
                                        if (sd.getDifficulty() == 0) {
                                            final String fulltitle = (sd.getTitle() + sd.getSubtitle()).toLowerCase();
                                            final String diffname = (sd.getSubtitle()).toLowerCase();
                                            if (diffname.contains("beginner")) {
                                                sd.setDifficulty(1);
                                            } else if (diffname.contains("normal")) {
                                                sd.setDifficulty(2);
                                            } else if (diffname.contains("hyper")) {
                                                sd.setDifficulty(3);
                                            } else if (diffname.contains("another")) {
                                                sd.setDifficulty(4);
                                            } else if (diffname.contains("insane") || diffname.contains("leggendaria")) {
                                                sd.setDifficulty(5);
                                            } else {
                                                if (fulltitle.contains("beginner")) {
                                                    sd.setDifficulty(1);
                                                } else if (fulltitle.contains("normal")) {
                                                    sd.setDifficulty(2);
                                                } else if (fulltitle.contains("hyper")) {
                                                    sd.setDifficulty(3);
                                                } else if (fulltitle.contains("another")) {
                                                    sd.setDifficulty(4);
                                                } else if (fulltitle.contains("insane") || fulltitle.contains("leggendaria")) {
                                                    sd.setDifficulty(5);
                                                } else {
                                                    if (sd.getNotes() < 250) {
                                                        sd.setDifficulty(1);
                                                    } else if (sd.getNotes() < 600) {
                                                        sd.setDifficulty(2);
                                                    } else if (sd.getNotes() < 1000) {
                                                        sd.setDifficulty(3);
                                                    } else if (sd.getNotes() < 2000) {
                                                        sd.setDifficulty(4);
                                                    } else {
                                                        sd.setDifficulty(5);
                                                    }
                                                }
                                            }
                                        }

                                        // BMSの#PREVIEWに記載がない、かつ、フォルダ内にpreviewがあった場合は、フォルダ内のpreviewをセットする
                                        // フォルダ内に実在するpreviewファイル名を優先したほうが良いような気が？
                                        if ((sd.getPreview() == null || sd.getPreview().length() == 0) && previewpath != null) {
                                            sd.setPreview(previewpath);
                                        }

                                        final String tag = property.tags.get(sd.getSha256());
                                        final Integer favorite = property.favorites.get(sd.getSha256());

                                        // PreparedStatement に直接パラメータをセットして addBatch() する（再利用）
                                        try {
                                            // 1..29 パラメータ（song テーブルの列順に合わせる）
                                            psSong.setString(1, sd.getMd5());
                                            psSong.setString(2, sd.getSha256());
                                            psSong.setString(3, sd.getTitle());
                                            psSong.setString(4, sd.getSubtitle());
                                            psSong.setString(5, sd.getGenre());
                                            psSong.setString(6, sd.getArtist());
                                            psSong.setString(7, sd.getSubartist());
                                            psSong.setString(8, tag != null ? tag : "");
                                            psSong.setString(9, pathname);
                                            psSong.setString(10, SongUtils.crc32(bmsPath.getParent().toString(), bmsroot, root.toString()));
                                            psSong.setString(11, sd.getStagefile());
                                            psSong.setString(12, sd.getBanner());
                                            psSong.setString(13, sd.getBackbmp());
                                            psSong.setString(14, sd.getPreview());
                                            psSong.setString(15, SongUtils.crc32(bmsPath.getParent().getParent().toString(), bmsroot, root.toString()));
                                            psSong.setInt(16, sd.getLevel());
                                            psSong.setInt(17, sd.getDifficulty());
                                            psSong.setInt(18, sd.getMaxbpm());
                                            psSong.setInt(19, sd.getMinbpm());
                                            psSong.setInt(20, sd.getLength());
                                            psSong.setInt(21, sd.getMode());
                                            psSong.setInt(22, sd.getJudge());
                                            psSong.setInt(23, sd.getFeature());
                                            psSong.setInt(24, sd.getContent());
                                            psSong.setInt(25, (int) lastModified);
                                            psSong.setInt(26, favorite != null ? favorite : 0);
                                            psSong.setInt(27, (int) property.updatetime);
                                            psSong.setInt(28, sd.getNotes());
                                            psSong.setString(29, sd.getCharthash());

                                            psSong.addBatch();
                                            int cur = pendingSong.incrementAndGet();
                                            if (cur >= BATCH_SIZE) {
                                                // flush
                                                flushSong.run();
                                            }
                                        } catch (SQLException e) {
                                            Logger.getGlobal().severe("song バッチ用パラメータ設定中の例外: " + e.getMessage());
                                        }

                                        // songinfo.dbのUPDATE
                                        if (property.info != null) {
                                            try { property.info.update(model); } catch (Throwable t) {}
                                        }
                                        property.count.incrementAndGet();
                                    } else {
                                        // ノーツ0かつwav0 -> 削除 (そもそもDBに登録されていないはず？)
                                        try {
                                            int deleted = qr.update(property.conn, "DELETE FROM song WHERE path = ?", pathname);
                                            if (deleted > 0) songDeleteCount.addAndGet(deleted);
                                        } catch (SQLException e) { e.printStackTrace(); }
                                    }
                                } // for bmsFiles

                                // 処理済み扱い: folderMap から除外
                                folderMap.remove(dirKey);

                                // このディレクトリの folder レコードをバッチに追加（直下BMSフォルダ）
                                try {
                                    Path parentpath = dir.getParent();
                                    if (parentpath == null) parentpath = dir.toAbsolutePath().getParent();
                                    // subtitle, command, banner, type, maxはオリジナル実装でもデフォルト値がセットされることになる
                                    psFolder.setString(1, dir.getFileName().toString());
                                    psFolder.setString(2, null);
                                    psFolder.setString(3, null);
                                    psFolder.setString(4, dirKey);
                                    psFolder.setString(5, null);
                                    psFolder.setString(6, SongUtils.crc32(parentpath.toString(), bmsroot, root.toString()));
                                    psFolder.setInt(7, 0);
                                    psFolder.setInt(8, (int) (int) dirMtime);
                                    psFolder.setInt(9, (int) property.updatetime);
                                    psFolder.setInt(10, 0);

                                    psFolder.addBatch();
                                    int fcur = pendingFolder.incrementAndGet();
                                    if (fcur >= BATCH_SIZE) {
                                        flushFolder.run();
                                    }
                                } catch (SQLException e) {
                                    Logger.getGlobal().severe("直下BMSフォルダの folder バッチ準備中の例外: " + e.getMessage());
                                }

                                // 直下BMSフォルダなのでサブツリーをスキップ
                                return FileVisitResult.SKIP_SUBTREE;
                            }

                            // 直下BMSがない途中ディレクトリの処理
                            @Override
                            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                                final String dirKey = (dir.startsWith(root) ? root.relativize(dir).toString() : dir.toString()) + File.separatorChar;
                                final FolderData original = folderMap.remove(dirKey);

                                Long cached = dirMtimeCache.remove(dirKey);
                                long dirDate;
                                if (cached != null) dirDate = cached;
                                else dirDate = Files.getLastModifiedTime(dir).toMillis() / 1000;

                                // DBにない or 更新日時が変わっている場合は挿入（バッチ）
                                if (original == null || original.getDate() != dirDate) {
                                    try {
                                        Path parentpath = dir.getParent();
                                        if (parentpath == null) parentpath = dir.toAbsolutePath().getParent();
                                        // subtitle, command, banner, type, maxはオリジナル実装でもデフォルト値がセットされることになる
                                        psFolder.setString(1, dir.getFileName().toString());
                                        psFolder.setString(2, null);
                                        psFolder.setString(3, null);
                                        psFolder.setString(4, dirKey);
                                        psFolder.setString(5, null);
                                        psFolder.setString(6, SongUtils.crc32(parentpath.toString(), bmsroot, root.toString()));
                                        psFolder.setInt(7, 0);
                                        psFolder.setInt(8, (int) dirDate);
                                        psFolder.setInt(9, (int) property.updatetime);
                                        psFolder.setInt(10, 0);

                                        psFolder.addBatch();
                                        int fcur = pendingFolder.incrementAndGet();
                                        if (fcur >= BATCH_SIZE) {
                                            flushFolder.run();
                                        }
                                    } catch (SQLException e) {
                                        Logger.getGlobal().severe("folder バッチ準備中の例外: " + e.getMessage());
                                    }
                                }

                                return FileVisitResult.CONTINUE;
                            }
                        }); // end walkFileTree(scanRoot)
                    } // end for each root

                    // 走査後: 残っているバッチを flush
                    flushSong.run();
                    flushFolder.run();

                    // 走査後、songMap に残っているものは実ファイルが存在しなかったレコードなので削除する
                    for (SongData leftover : songMap.values()) {
                        try {
                            int deleted = qr.update(property.conn, "DELETE FROM song WHERE path = ?", leftover.getPath());
                            if (deleted > 0) songDeleteCount.addAndGet(deleted);
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }

                    // 同様に、folderMap に残っているフォルダは存在しないので削除する
                    for (FolderData leftover : folderMap.values()) {
                        try {
                            int fd = qr.update(property.conn, "DELETE FROM folder WHERE path LIKE ?", leftover.getPath() + "%");
                            if (fd > 0) folderDeleteCount.addAndGet(fd);
                            int sd = qr.update(property.conn, "DELETE FROM song WHERE path LIKE ?", leftover.getPath() + "%");
                            if (sd > 0) songDeleteCount.addAndGet(sd);
                        } catch (SQLException e) {
                            Logger.getGlobal().severe("ディレクトリ内に存在しないフォルダレコード削除の例外:" + e.getMessage());
                        }
                    }
                    // commit は外側で行う（try-with-resources の conn を使用）
                } // end try-with-resources psSong/psFolder
                conn.commit();
			} catch (Exception e) {
				Logger.getGlobal().severe("楽曲データベース更新時の例外:" + e.getMessage());
				e.printStackTrace();
			} // songdata.db用トランザクション終了 try-with-resources

			if(info != null) {
                // songinfo.db用トランザクション終了
				info.endUpdate();
			}
			long nowtime = System.currentTimeMillis();
			Logger.getGlobal().info("楽曲更新完了 : Time - " + (nowtime - time) + " 1曲あたりの時間 - "
					+ (property.count.get() > 0 ? (nowtime - time) / property.count.get() : "不明"));
            Logger.getGlobal().info("DB 操作件数: song insert=" + songInsertCount.get() + " update=" + songUpdateCount.get() + " delete=" + songDeleteCount.get()
                    + " | folder insert=" + folderInsertCount.get() + " delete=" + folderDeleteCount.get());
		}
	}

	private static class SongDatabaseUpdaterProperty {
		private final Map<String, String> tags = new HashMap<String, String>();
		private final Map<String, Integer> favorites = new HashMap<String, Integer>();
		private final SongInformationAccessor info;
		private final long updatetime;
		private final AtomicInteger count = new AtomicInteger();
		private Connection conn;
		
		public SongDatabaseUpdaterProperty(long updatetime, SongInformationAccessor info) {
			this.updatetime = updatetime;
			this.info = info;
		}
	}
	
	public static interface SongDatabaseAccessorPlugin {
		public void update(BMSModel model, SongData song);
	}
}
