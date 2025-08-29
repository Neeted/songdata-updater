package bms.player.beatoraja.song;

import jp.howan.songdata.EverythingBatchIndexer;
import jp.howan.songdata.EverythingSearchResult;
import jp.howan.songdata.BmsFolder;

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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
//import java.util.stream.Collectors;
import java.util.stream.Stream;

import bms.player.beatoraja.SQLiteDatabaseAccessor;
//import bms.player.beatoraja.Validatable;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteConfig.SynchronousMode;
import org.sqlite.SQLiteDataSource;

import bms.model.*;

import static jp.howan.songdata.EverythingFacade.EverythingIsAvailable;

/**
 * 楽曲データベースへのアクセスクラス
 * 
 * @author exch
 */
public class SQLiteSongDatabaseAccessor extends SQLiteDatabaseAccessor implements SongDatabaseAccessor {

	private final SQLiteDataSource ds;

	private final Path root;

	private final ResultSetHandler<List<SongData>> songhandler = new BeanListHandler<>(SongData.class);
	private final ResultSetHandler<List<FolderData>> folderhandler = new BeanListHandler<>(FolderData.class);

	private final QueryRunner qr;
	
//	private List<SongDatabaseAccessorPlugin> plugins = new ArrayList();
	
	public SQLiteSongDatabaseAccessor(String filepath) throws ClassNotFoundException {
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
		conf.setJournalMode(SQLiteConfig.JournalMode.MEMORY);
        conf.setTempStore(SQLiteConfig.TempStore.MEMORY);
        conf.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
        conf.setCacheSize(-50000);
		ds = new SQLiteDataSource(conf);
		ds.setUrl("jdbc:sqlite:" + filepath);
		qr = new QueryRunner(ds);
		root = Paths.get(".");
		createTable();
	}
		
//	public void addPlugin(SongDatabaseAccessorPlugin plugin) {
//		plugins.add(plugin);
//	}
	
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

	
//	/**
//	 * 楽曲を取得する
//	 *
//	 * @param key
//	 *            属性
//	 * @param value
//	 *            属性値
//	 * @return 検索結果
//	 */
//	public SongData[] getSongDatas(String key, String value) {
//		try {
//			final List<SongData> m = qr.query("SELECT * FROM song WHERE " + key + " = ?", songhandler, value);
//			return Validatable.removeInvalidElements(m).toArray(new SongData[m.size()]);
//		} catch (Exception e) {
//			e.printStackTrace();
//			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
//		}
//		return SongData.EMPTY;
//	}
//
//	/**
//	 * MD5/SHA256で指定した楽曲をまとめて取得する
//	 *
//	 * @param hashes
//	 *            楽曲のMD5/SHA256
//	 * @return 取得した楽曲
//	 */
//	public SongData[] getSongDatas(String[] hashes) {
//		try {
//			StringBuilder md5str = new StringBuilder();
//			StringBuilder sha256str = new StringBuilder();
//			for (String hash : hashes) {
//				if (hash.length() > 32) {
//					if (sha256str.length() > 0) {
//						sha256str.append(',');
//					}
//					sha256str.append('\'').append(hash).append('\'');
//				} else {
//					if (md5str.length() > 0) {
//						md5str.append(',');
//					}
//					md5str.append('\'').append(hash).append('\'');
//				}
//			}
//			List<SongData> m = qr.query("SELECT * FROM song WHERE md5 IN (" + md5str.toString() + ") OR sha256 IN ("
//					+ sha256str.toString() + ")", songhandler);
//
//			// 検索並び順保持
//			List<SongData> sorted = m.stream().sorted((a, b) -> {
//				int aIndexSha256 = -1,aIndexMd5 = -1,bIndexSha256 = -1,bIndexMd5 = -1;
//				for(int i = 0;i < hashes.length;i++) {
//					if(hashes[i].equals(a.getSha256())) aIndexSha256 = i;
//					if(hashes[i].equals(a.getMd5())) aIndexMd5 = i;
//					if(hashes[i].equals(b.getSha256())) bIndexSha256 = i;
//					if(hashes[i].equals(b.getMd5())) bIndexMd5 = i;
//				}
//			    int aIndex = Math.min((aIndexSha256 == -1 ? Integer.MAX_VALUE : aIndexSha256), (aIndexMd5 == -1 ? Integer.MAX_VALUE : aIndexMd5));
//			    int bIndex = Math.min((bIndexSha256 == -1 ? Integer.MAX_VALUE : bIndexSha256), (bIndexMd5 == -1 ? Integer.MAX_VALUE : bIndexMd5));
//			    return bIndex - aIndex;
//            }).collect(Collectors.toList());
//
//			SongData[] validated = Validatable.removeInvalidElements(sorted).toArray(new SongData[m.size()]);
//			return validated;
//		} catch (Exception e) {
//			e.printStackTrace();
//			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
//		}
//
//		return SongData.EMPTY;
//	}
//
//	public SongData[] getSongDatas(String sql, String score, String scorelog, String info) {
//		try (Statement stmt = qr.getDataSource().getConnection().createStatement()) {
//			stmt.execute("ATTACH DATABASE '" + score + "' as scoredb");
//			stmt.execute("ATTACH DATABASE '" + scorelog + "' as scorelogdb");
//			List<SongData> m;
//
//			if(info != null) {
//				stmt.execute("ATTACH DATABASE '" + info + "' as infodb");
//				String s = "SELECT DISTINCT md5, song.sha256 AS sha256, title, subtitle, genre, artist, subartist,path,folder,stagefile,banner,backbmp,parent,level,difficulty,"
//						+ "maxbpm,minbpm,song.mode AS mode, judge, feature, content, song.date AS date, favorite, song.notes AS notes, adddate, preview, length, charthash"
//						+ " FROM song INNER JOIN (information LEFT OUTER JOIN (score LEFT OUTER JOIN scorelog ON score.sha256 = scorelog.sha256) ON information.sha256 = score.sha256) "
//						+ "ON song.sha256 = information.sha256 WHERE " + sql;
//				ResultSet rs = stmt.executeQuery(s);
//				m = songhandler.handle(rs);
////				System.out.println(s + " -> result : " + m.size());
//				stmt.execute("DETACH DATABASE infodb");
//			} else {
//				String s = "SELECT DISTINCT md5, song.sha256 AS sha256, title, subtitle, genre, artist, subartist,path,folder,stagefile,banner,backbmp,parent,level,difficulty,"
//						+ "maxbpm,minbpm,song.mode AS mode, judge, feature, content, song.date AS date, favorite, song.notes AS notes, adddate, preview, length, charthash"
//						+ " FROM song LEFT OUTER JOIN (score LEFT OUTER JOIN scorelog ON score.sha256 = scorelog.sha256) ON song.sha256 = score.sha256 WHERE " + sql;
//				ResultSet rs = stmt.executeQuery(s);
//				m = songhandler.handle(rs);
//			}
//			stmt.execute("DETACH DATABASE scorelogdb");
//			stmt.execute("DETACH DATABASE scoredb");
//			return Validatable.removeInvalidElements(m).toArray(new SongData[m.size()]);
//		} catch(Throwable e) {
//			e.printStackTrace();
//		}
//
//		return SongData.EMPTY;
//
//	}
//
//	public SongData[] getSongDatasByText(String text) {
//		try {
//			List<SongData> m = qr.query(
//					"SELECT * FROM song WHERE rtrim(title||' '||subtitle||' '||artist||' '||subartist||' '||genre) LIKE ?"
//							+ " GROUP BY sha256",songhandler, "%" + text + "%");
//			return Validatable.removeInvalidElements(m).toArray(new SongData[m.size()]);
//		} catch (Exception e) {
//			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
//		}
//
//		return SongData.EMPTY;
//	}
//
//	/**
//	 * 楽曲を取得する
//	 *
//	 * @param key
//	 *            属性
//	 * @param value
//	 *            属性値
//	 * @return 検索結果
//	 */
//	public FolderData[] getFolderDatas(String key, String value) {
//		try {
//			final List<FolderData> m = qr.query("SELECT * FROM folder WHERE " + key + " = ?", folderhandler, value);
//			return m.toArray(new FolderData[m.size()]);
//		} catch (Exception e) {
//			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
//		}
//
//		return FolderData.EMPTY;
//	}
//
//	/**
//	 * 楽曲を更新する
//	 *
//	 * @param songs 更新する楽曲
//	 */
//	public void setSongDatas(SongData[] songs) {
//		try (Connection conn = qr.getDataSource().getConnection()){
//			conn.setAutoCommit(false);
//
//			for (SongData sd : songs) {
//				this.insert(qr, conn, "song", sd);
//			}
//			conn.commit();
//			conn.close();
//		} catch (Exception e) {
//			Logger.getGlobal().severe("song.db更新時の例外:" + e.getMessage());
//		}
//	}

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
        if (EverythingIsAvailable()) {
            Logger.getGlobal().info("Everythingをファイル検索に利用します(Everythingのインデックスは最新化されている前提です)");
            updater.updateSongDatasWithEverything();
        } else {
            Logger.getGlobal().info("Everythingが利用できないので通常のディレクトリ走査を行います");
		    updater.updateSongDatas(path == null ? Stream.of(bmsroot).map(Paths::get) : Stream.of(Paths.get(path)));
        }
	}
	
	/**
	 * song database更新用クラス
	 * 
	 * @author exch
	 */
	class SongDatabaseUpdater {

		private final boolean updateAll;
		private final String[] bmsroot;
		private final SongInformationAccessor info;
        
        // DB操作件数カウンタ
        private final AtomicInteger songInsertCount = new AtomicInteger(0);
        private final AtomicInteger songUpdateCount = new AtomicInteger(0);
        private final AtomicInteger songDeleteCount = new AtomicInteger(0);
        private final AtomicInteger folderInsertCount = new AtomicInteger(0);
        private final AtomicInteger folderDeleteCount = new AtomicInteger(0);

        private final int BATCH_SIZE = 1000; // バッチサイズ（必要に応じて調整）
        // SQL 文（テーブル定義の列順に合わせること）
        private final String songInsertSQL = "INSERT OR REPLACE INTO song (md5, sha256, title, subtitle, genre, artist, subartist, tag, path, folder, stagefile, banner, backbmp, preview, parent, level, difficulty, maxbpm, minbpm, length, mode, judge, feature, content, date, favorite, adddate, notes, charthash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        private final String folderInsertSQL = "INSERT OR REPLACE INTO folder (title, subtitle, command, path, banner, parent, type, date, adddate, max) VALUES (?,?,?,?,?,?,?,?,?,?)";

        private final long updatetime = Calendar.getInstance().getTimeInMillis() / 1000;
        private final long starttime = System.currentTimeMillis();
        private final AtomicInteger newBmsCount = new AtomicInteger(0);

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
			if(info != null) {
                // songinfo.db用トランザクション開始
				info.startUpdate();
			}
            
            // songdata.db用トランザクション開始
			try (Connection conn = ds.getConnection();
                 Statement st = conn.createStatement()) {
				conn.setAutoCommit(false);
                // songdata.dbをメモリに乗せる
                st.execute("PRAGMA mmap_size=500000000;");

                // 楽曲のタグ,FAVORITEの保持
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの保持を開始します");
                // 一時テーブル作成（sha256 PRIMARY KEY）
                st.executeUpdate(
                "CREATE TEMP TABLE temp_tags(sha256 TEXT PRIMARY KEY, tag TEXT, favorite INTEGER);");
                // songのsha256が重複していた場合、最後に現れる行が最終値になる
                st.executeUpdate(
                "INSERT OR REPLACE INTO temp_tags (sha256, tag, favorite) " +
                    "SELECT sha256, tag, favorite FROM song " +
                    "WHERE (tag IS NOT NULL AND tag <> '') OR favorite > 0 " +
                    "ORDER BY rowid;"
                );
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの保持が完了しました");

				if(updateAll) {
                    // 楽曲全更新、既存テーブルをDELEETE
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
									+ dsql, param);
					qr.update(conn, "DELETE FROM song WHERE " + dsql, param);
				}

                // --- 2つの処理を別スレッドで並列実行 ---
                Logger.getGlobal().info("フォルダ走査に必要なデータ構築を並列で行います");
                final ExecutorService executor = Executors.newFixedThreadPool(2);

                CompletableFuture<NavigableMap<String, SongData>> songsFuture =
                    CompletableFuture.supplyAsync(() -> {
                        Logger.getGlobal().info("songテーブルのハッシュマップ構築を開始します");
                        NavigableMap<String, SongData> songTbMap = new TreeMap<>();
                        try {
                            List<SongData> allSongs = qr.query(conn, "SELECT path, date, preview, sha256 FROM song", songhandler);
                            if (allSongs != null) {
                                for (SongData s : allSongs) {
                                    if (s != null && s.getPath() != null) songTbMap.put(s.getPath(), s);
                                }
                            }
                        } catch (SQLException e) {
                            throw new CompletionException(e);
                        }
                        Logger.getGlobal().info("songテーブルのハッシュマップ構築が完了しました");
                        return songTbMap;
                    }, executor);

                CompletableFuture<Map<String, FolderData>> foldersFuture =
                    CompletableFuture.supplyAsync(() -> {
                        Logger.getGlobal().info("folderテーブルのハッシュマップ構築を開始します");
                        Map<String, FolderData> folderTbMap = new HashMap<>();
                        try {
                            List<FolderData> allFolders = qr.query(conn, "SELECT path, date, parent FROM folder", folderhandler);
                            if (allFolders != null) {
                                for (FolderData f : allFolders) {
                                    if (f != null && f.getPath() != null) folderTbMap.put(f.getPath(), f);
                                }
                            }
                        } catch (SQLException e) {
                            throw new CompletionException(e);
                        }
                        Logger.getGlobal().info("folderテーブルのハッシュマップ構築が完了しました");
                        return folderTbMap;
                    }, executor);

                // 全部揃うまで待つ（例外は unwrap して再スロー）
                try {
                    CompletableFuture<Void> all = CompletableFuture.allOf(songsFuture, foldersFuture);
                    all.join();
                } catch (CompletionException ex) {
                    // 並列処理で例外が発生した場合は Executor を停止し、原因を unwrap して投げる
                    executor.shutdownNow();
                    Throwable cause = ex.getCause();
                    if (cause instanceof RuntimeException) throw (RuntimeException) cause;
                    if (cause instanceof Error) throw (Error) cause;
                    if (cause instanceof Exception) throw (Exception) cause;
                    throw ex;
                }

                // Executor の終了処理
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException ie) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }

                // 並列結果を個別の変数に取り出す
                final NavigableMap<String, SongData> songTbMap = songsFuture.join();
                final Map<String, FolderData> folderTbMap = foldersFuture.join();

                Logger.getGlobal().info("フォルダ走査に必要なデータ構築の並列処理が完了しました");

                // PreparedStatement を作って再利用する（ラムダ内から参照するため final）try-with-resources psSong/psFolder
                try (final PreparedStatement psSong = conn.prepareStatement(songInsertSQL);
                     final PreparedStatement psFolder = conn.prepareStatement(folderInsertSQL)) {

                    // バッチのペンディング件数（ラムダから変更できるよう AtomicInteger を使う）
                    final AtomicInteger pendingSong = new AtomicInteger(0);
                    final AtomicInteger pendingFolder = new AtomicInteger(0);

                    // 小ヘルパ: song バッチ flush（PreparedStatement を executeBatch）
                    final Runnable flushSong = () -> {
                        int toFlush = pendingSong.get();
                        if (toFlush <= 0) return;
                        try {
                            // 実行
                            psSong.executeBatch();
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
                            psFolder.executeBatch();
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

                    // ルートごとにツリーを走査（preVisitDirectory で完結）
                    for (Iterator<Path> it = paths.iterator(); it.hasNext();) {
                        final Path scanRoot = it.next();
                        Logger.getGlobal().info("走査中ルート: " + scanRoot.toString());

                        Files.walkFileTree(scanRoot, new SimpleFileVisitor<>() {
                            @Override
                            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                                final String dirKey = (dir.startsWith(root) ? root.relativize(dir).toString() : dir.toString()) + File.separatorChar;
                                final FolderData folderRecord = folderTbMap.get(dirKey);
                                final long dirModTime = attrs.lastModifiedTime().toMillis() / 1000;

                                // ディレクトリ直下ファイル確認(途中ディレクトリかBMSフォルダか判定、preview音源やtxtがあるか)
                                List<Path> bmsFiles = new ArrayList<>();
                                boolean hasTxt = false;
                                String previewFileName = null;
                                boolean isUpdateDir = folderRecord == null || folderRecord.getDate() != dirModTime;

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
                                                if (previewFileName == null) {
                                                    if (lname.startsWith("preview") && (lname.endsWith(".wav") || lname.endsWith(".ogg") || lname.endsWith(".mp3") || lname.endsWith(".flac"))) {
                                                        previewFileName = name;
                                                    }
                                                }
                                                if (!hasTxt && lname.endsWith(".txt")) hasTxt = true;
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

                                // 走査フォルダがfolderテーブルにある場合は取得しDELETE対象から外す
                                final FolderData folderDataFromTb = folderTbMap.remove(dirKey);

                                // DBにない or 更新日時が変わっている場合はこのディレクトリの folder レコードをバッチに追加
                                if (folderDataFromTb == null || folderDataFromTb.getDate() != dirModTime) {
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
                                        psFolder.setInt(8, (int) dirModTime);
                                        psFolder.setInt(9, (int) updatetime);
                                        psFolder.setInt(10, 0);

                                        psFolder.addBatch();
                                        int fcur = pendingFolder.incrementAndGet();
                                        if (fcur >= BATCH_SIZE) {
                                            flushFolder.run();
                                        }
                                    } catch (SQLException e) {
                                        Logger.getGlobal().log(Level.SEVERE, "folder バッチ準備中の例外: ", e);
                                    }
                                }

                                // 直下 BMS がない途中ディレクトリの場合は通常走査
                                if (bmsFiles.isEmpty()) return FileVisitResult.CONTINUE;

                                // [SKIP_SUBTREE] 更新なしフォルダケース: 直下のBMSを処理済みにしてDELETE対象から外す
                                if (!isUpdateDir) {
                                    folderTbMap.remove(dirKey);
                                    // 更新なしフォルダ以下のBMSを処理済みにする
                                    NavigableMap<String, SongData> tail = songTbMap.tailMap(dirKey, true);
                                    Iterator<String> tit = tail.keySet().iterator();
                                    while (tit.hasNext()) {
                                        String key = tit.next();
                                        if (!key.startsWith(dirKey)) break;
                                        tit.remove();
                                    }
                                    return FileVisitResult.SKIP_SUBTREE;
                                }

                                // 更新ありフォルダケース: 各BMSを処理
                                for (Path bmsPath : bmsFiles) {
                                    final String pathname = (bmsPath.startsWith(root) ? root.relativize(bmsPath).toString() : bmsPath.toString());
                                    long bmsModTime = -1;
                                    try {
                                        bmsModTime = Files.getLastModifiedTime(bmsPath).toMillis() / 1000;
                                    } catch (IOException e) {
                                        Logger.getGlobal().log(Level.SEVERE, "BMSファイル更新時間取得の例外", e);
                                    }

                                    // 対象BMSを処理済みにして、songのDELETE対象から外す
                                    final SongData songDataFromTb = songTbMap.remove(pathname);
                                    // 既存BMS(フルパスと更新日時が一致)の場合はpreview音源のみ更新処理する
                                    if (songDataFromTb != null && songDataFromTb.getDate() == bmsModTime) {
                                        final String oldpp = songDataFromTb.getPreview() == null ? "" : songDataFromTb.getPreview();
                                        final String newpp = previewFileName == null ? "" : previewFileName;
                                        // DBのpreviewとフォルダ内のpreviewが一致していない、かつ、フォルダ内のpreviewが空じゃない場合は、フォルダ内のpreviewをsongにUPDATEする
                                        // 単に一致しない場合新しいものにすると、#PREVIEW _preview.wavのように指定されていた時に、音源が消えてしまうことになるので実在する場合のみ更新
                                        if (!oldpp.equals(newpp) && !newpp.isEmpty()) {
                                            try {
                                                int updated = qr.update(conn, "UPDATE song SET preview=? WHERE path = ?", newpp, pathname);
                                                if (updated > 0) songUpdateCount.addAndGet(updated);
                                            } catch (SQLException e) {
                                                Logger.getGlobal().warning("Error while updating preview at " + pathname + ": " + e.getMessage());
                                            }
                                        }
                                        continue; // 既存BMSはpreview音源のみ更新して次のBMSの処理へ
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
                                        Logger.getGlobal().log(Level.SEVERE, "Error while decoding " + pathname + ": " + e.getMessage(), e);
                                    }
                                    if (model == null) continue; // デコードできなかったらcontinue

                                    SongData sd;
                                    try {
                                        sd = new SongData(model, hasTxt);
                                    } catch (Throwable t) {
                                        Logger.getGlobal().log(Level.SEVERE, "SongData 生成失敗 : path=" + pathname + " cause=" + t.getMessage(), t);
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
                                        if ((sd.getPreview() == null || sd.getPreview().isEmpty()) && previewFileName != null) {
                                            sd.setPreview(previewFileName);
                                        }

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
                                            psSong.setString(8, "");
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
                                            psSong.setInt(25, (int) bmsModTime);
                                            psSong.setInt(26, 0);
                                            psSong.setInt(27, (int) updatetime);
                                            psSong.setInt(28, sd.getNotes());
                                            psSong.setString(29, sd.getCharthash());

                                            psSong.addBatch();
                                            int cur = pendingSong.incrementAndGet();
                                            if (cur >= BATCH_SIZE) {
                                                flushSong.run();
                                            }
                                        } catch (SQLException e) {
                                            Logger.getGlobal().log(Level.SEVERE, "song バッチ用パラメータ設定中の例外: ", e);
                                        }

                                        // songinfo.dbのUPDATE
                                        if (info != null) info.update(model);

                                        newBmsCount.incrementAndGet();
                                    } else {
                                        // ノーツ0かつwav0 -> 削除 (そもそもDBに登録されていないはず？)
                                        try {
                                            int deleted = qr.update(conn, "DELETE FROM song WHERE path = ?", pathname);
                                            if (deleted > 0) songDeleteCount.addAndGet(deleted);
                                        } catch (SQLException e) {
                                            Logger.getGlobal().log(Level.SEVERE, "songレコード削除の例外", e);
                                        }
                                    }
                                } // for bmsFiles

                                // 直下BMSフォルダなのでサブツリーをスキップ
                                return FileVisitResult.SKIP_SUBTREE;
                            }
                        }); // end walkFileTree(scanRoot)
                    } // end for each root

                    // 走査後: 残っているバッチを flush
                    flushSong.run();
                    flushFolder.run();

                    // 走査後、songTbMap に残っているものは実ファイルが存在しないレコードなので削除
                    for (SongData leftover : songTbMap.values()) {
                        try {
                            int deleted = qr.update(conn, "DELETE FROM song WHERE path = ?", leftover.getPath());
                            if (deleted > 0) songDeleteCount.addAndGet(deleted);
                        } catch (SQLException e) {
                            Logger.getGlobal().log(Level.SEVERE, "ディレクトリ内に存在しないsongレコード削除の例外", e);
                        }
                    }

                    // 同様に、folderTbMap に残っているものは実フォルダが存在しないレコードなので削除
                    for (FolderData leftover : folderTbMap.values()) {
                        try {
                            int fd = qr.update(conn, "DELETE FROM folder WHERE path LIKE ?", leftover.getPath() + "%");
                            if (fd > 0) folderDeleteCount.addAndGet(fd);
                            int sd = qr.update(conn, "DELETE FROM song WHERE path LIKE ?", leftover.getPath() + "%");
                            if (sd > 0) songDeleteCount.addAndGet(sd);
                        } catch (SQLException e) {
                            Logger.getGlobal().log(Level.SEVERE, "ディレクトリ内に存在しないfolderレコード削除の例外:", e);
                        }
                    }
                    // commit は外側で行う（try-with-resources の conn を使用）
                } // end try-with-resources psSong/psFolder

                // 楽曲のタグ,FAVORITEのを一時テーブルから復元
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの復元を開始します");
                st.executeUpdate(
                "UPDATE song SET " +
                    "tag = COALESCE((SELECT tag FROM temp_tags WHERE temp_tags.sha256 = song.sha256), song.tag), " +
                    "favorite = COALESCE((SELECT favorite FROM temp_tags WHERE temp_tags.sha256 = song.sha256), song.favorite) " +
                    "WHERE EXISTS (SELECT 1 FROM temp_tags WHERE temp_tags.sha256 = song.sha256);"
                );
                st.executeUpdate("DROP TABLE IF EXISTS temp_tags;");
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの復元が完了しました");

                conn.commit();
			} catch (Exception e) {
                Logger.getGlobal().log(Level.SEVERE, "楽曲データベース更新時の例外", e);
			} // songdata.db用トランザクション終了 try-with-resources

			if(info != null) {
                // songinfo.db用トランザクション終了
				info.endUpdate();
			}
			long nowtime = System.currentTimeMillis();
			Logger.getGlobal().info("楽曲更新完了 : Time - " + (nowtime - starttime) + " 1曲あたりの時間 - "
					+ (newBmsCount.get() > 0 ? (nowtime - starttime) / newBmsCount.get() : "不明"));
            Logger.getGlobal().info("DB 操作件数: song insert=" + songInsertCount.get() + " update=" + songUpdateCount.get() + " delete=" + songDeleteCount.get()
                    + " | folder insert=" + folderInsertCount.get() + " delete=" + folderDeleteCount.get());
		}

        /**
         * データベースを更新する(Everything連携専用)
         */
        public void updateSongDatasWithEverything() {
            if(info != null) {
                // songinfo.db用トランザクション開始
                info.startUpdate();
            }

            // songdata.db用トランザクション開始
            try (Connection conn = ds.getConnection();
                 Statement st = conn.createStatement()) {
                conn.setAutoCommit(false);
                // songdata.dbをメモリに乗せる
                st.execute("PRAGMA mmap_size=500000000;");

                // 楽曲のタグ,FAVORITEの保持
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの保持を開始します");
                // 一時テーブル作成（sha256 PRIMARY KEY）
                st.executeUpdate(
                "CREATE TEMP TABLE temp_tags(sha256 TEXT PRIMARY KEY, tag TEXT, favorite INTEGER);");
                // songのsha256が重複していた場合、最後に現れる行が最終値になる
                st.executeUpdate(
                "INSERT OR REPLACE INTO temp_tags (sha256, tag, favorite) " +
                    "SELECT sha256, tag, favorite FROM song " +
                    "WHERE (tag IS NOT NULL AND tag <> '') OR favorite > 0 " +
                    "ORDER BY rowid;"
                );
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの保持が完了しました");

                if(updateAll) {
                    // 楽曲全更新、既存テーブルをDELEETE
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
                                    + dsql, param);
                    qr.update(conn, "DELETE FROM song WHERE " + dsql, param);
                }

                // --- 4つの処理を別スレッドで並列実行 ---
                Logger.getGlobal().info("フォルダ走査に必要なデータ構築を並列で行います");
                final ExecutorService executor = Executors.newFixedThreadPool(4);
                EverythingBatchIndexer indexer = new EverythingBatchIndexer(this.bmsroot);

                CompletableFuture<NavigableMap<String, SongData>> songsFuture =
                    CompletableFuture.supplyAsync(() -> {
                        Logger.getGlobal().info("songテーブルのハッシュマップ構築を開始します");
                        NavigableMap<String, SongData> songTbMap = new TreeMap<>();
                        try {
                            List<SongData> allSongs = qr.query(conn, "SELECT path, date, preview, sha256 FROM song", songhandler);
                            if (allSongs != null) {
                                for (SongData s : allSongs) {
                                    if (s != null && s.getPath() != null) songTbMap.put(s.getPath(), s);
                                }
                            }
                        } catch (SQLException e) {
                            throw new CompletionException(e);
                        }
                        Logger.getGlobal().info("songテーブルのハッシュマップ構築が完了しました");
                        return songTbMap;
                    }, executor);

                CompletableFuture<Map<String, FolderData>> foldersFuture =
                    CompletableFuture.supplyAsync(() -> {
                        Logger.getGlobal().info("folderテーブルのハッシュマップ構築を開始します");
                        Map<String, FolderData> folderTbMap = new HashMap<>();
                        try {
                            List<FolderData> allFolders = qr.query(conn, "SELECT path, date, parent FROM folder", folderhandler);
                            if (allFolders != null) {
                                for (FolderData f : allFolders) {
                                    if (f != null && f.getPath() != null) folderTbMap.put(f.getPath(), f);
                                }
                            }
                        } catch (SQLException e) {
                            throw new CompletionException(e);
                        }
                        Logger.getGlobal().info("folderテーブルのハッシュマップ構築が完了しました");
                        return folderTbMap;
                    }, executor);

                CompletableFuture<Map<Path, BmsFolder>> bmsFolderFuture =
                    CompletableFuture.supplyAsync(() -> {
                        Logger.getGlobal().info("EverythingでBMS/.txt/preview音源を検索しBMSフォルダ情報を構築します");
                        Map<Path, BmsFolder> m = indexer.buildBmsFolderMap();
                        Logger.getGlobal().info("EverythingによるBMSフォルダ情報構築完了");
                        return m;
                    }, executor);

                CompletableFuture<NavigableMap<Path, Long>> scanFoldersFuture =
                    CompletableFuture.supplyAsync(() -> {
                        Logger.getGlobal().info("Everythingで走査フォルダ一覧を取得します");
                        NavigableMap<Path, Long> m;
                        try {
                            m = indexer.buildScanFolders();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        Logger.getGlobal().info("Everythingによる走査フォルダ一覧取得完了");
                        return m;
                    }, executor);

                // 全部揃うまで待つ（例外は unwrap して再スロー）
                try {
                    CompletableFuture<Void> all = CompletableFuture.allOf(songsFuture, foldersFuture, bmsFolderFuture, scanFoldersFuture);
                    all.join();
                } catch (CompletionException ex) {
                    // 並列処理で例外が発生した場合は Executor を停止し、原因を unwrap して投げる
                    executor.shutdownNow();
                    Throwable cause = ex.getCause();
                    if (cause instanceof RuntimeException) throw (RuntimeException) cause;
                    if (cause instanceof Error) throw (Error) cause;
                    if (cause instanceof Exception) throw (Exception) cause;
                    throw ex;
                }

                // Executor の終了処理
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException ie) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }

                // 並列結果を個別の変数に取り出す
                final NavigableMap<String, SongData> songTbMap = songsFuture.join();
                final Map<String, FolderData> folderTbMap = foldersFuture.join();
                final Map<Path, BmsFolder> bmsFolderMap = bmsFolderFuture.join();
                final NavigableMap<Path, Long> scanFolders = scanFoldersFuture.join();

                Logger.getGlobal().info("フォルダ走査に必要なデータ構築の並列処理が完了しました");

                // PreparedStatement を作って再利用する（ラムダ内から参照するため final）try-with-resources psSong/psFolder
                try (final PreparedStatement psSong = conn.prepareStatement(songInsertSQL);
                     final PreparedStatement psFolder = conn.prepareStatement(folderInsertSQL)) {

                    // バッチのペンディング件数（ラムダから変更できるよう AtomicInteger を使う）
                    final AtomicInteger pendingSong = new AtomicInteger(0);
                    final AtomicInteger pendingFolder = new AtomicInteger(0);

                    // 小ヘルパ: song バッチ flush（PreparedStatement を executeBatch）
                    final Runnable flushSong = () -> {
                        int toFlush = pendingSong.get();
                        if (toFlush <= 0) return;
                        try {
                            // 実行
                            psSong.executeBatch();
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
                            psFolder.executeBatch();
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

                    // フォルダ走査ループ
                    Logger.getGlobal().info("フォルダ走査を開始します 走査対象フォルダ数: " + scanFolders.size() + " BMSリソース含有フォルダ数: " + bmsFolderMap.size());
                    while (!scanFolders.isEmpty()){
                        Map.Entry<Path, Long> entry = scanFolders.pollFirstEntry();
                        final Path dir = entry.getKey();
                        final String dirKey = (dir.startsWith(root) ? root.relativize(dir).toString() : dir.toString()) + File.separatorChar;
                        final FolderData folderTbRecord = folderTbMap.get(dirKey);
                        final long dirModTime  = entry.getValue();

                        // ディレクトリ直下ファイル確認(途中ディレクトリかBMSフォルダか判定、preview音源やtxtがあるか)
                        List<EverythingSearchResult> bmsFiles;
                        boolean hasTxt = false;
                        String previewFileName = null;
                        boolean isUpdateDir = folderTbRecord == null || folderTbRecord.getDate() != dirModTime;
                        if (isUpdateDir) {
                            // 更新ありフォルダ
                            BmsFolder bf = bmsFolderMap.get(dir);
                            bmsFiles = bf != null ? bf.bmsFiles : Collections.emptyList();
                            hasTxt = bf != null && bf.hasTxt;
                            List<Path> previewFiles = bf != null ? bf.previewFiles : Collections.emptyList();
                            previewFileName = previewFiles.isEmpty() ? null : previewFiles.get(0).getFileName().toString();
                        } else {
                            // 更新なしフォルダ
                            BmsFolder bf = bmsFolderMap.get(dir);
                            bmsFiles = bf != null ? bf.bmsFiles : Collections.emptyList();
                        }

                        // 走査フォルダがfolderテーブルにある場合は取得しDELETE対象から外す
                        final FolderData folderDataFromTb = folderTbMap.remove(dirKey);

                        // DBにない or 更新日時が変わっている場合はこのディレクトリの folder レコードをバッチに追加
                        if (folderDataFromTb == null || folderDataFromTb.getDate() != dirModTime) {
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
                                psFolder.setInt(8, (int) dirModTime);
                                psFolder.setInt(9, (int) updatetime);
                                psFolder.setInt(10, 0);

                                psFolder.addBatch();
                                int fcur = pendingFolder.incrementAndGet();
                                if (fcur >= BATCH_SIZE) {
                                    flushFolder.run();
                                }
                            } catch (SQLException e) {
                                Logger.getGlobal().log(Level.SEVERE,"folder バッチ準備中の例外: ", e);
                            }
                        }

                        // 直下BMSがない途中ディレクトリの場合はcontinue
                        if (bmsFiles.isEmpty()) continue;

                        // [SKIP_SUBTREE] 直下BMSフォルダなので、サブディレクトリが存在する場合走査対象から外す
                        NavigableMap<Path, Long> foldersTail = scanFolders.tailMap(dir, true);
                        Iterator<Path> ftit = foldersTail.keySet().iterator();
                        while (ftit.hasNext()) {
                            // dirkey = dir + File.separatorChar
                            String key = ftit.next().toString();
                            // パス先頭が「dir\」でも「dir」でもない場合は、子孫でも兄弟的フォルダでもないのでbreak
                            if (!key.startsWith(dirKey) && !key.startsWith(dir.toString())) break;
                            // パス先頭が「dir\」の場合子孫なので削除
                            if (key.startsWith(dirKey)) ftit.remove();
                        }

                        // 更新なしフォルダケース: 直下のBMSを処理済みにしてDELETE対象から外す
                        if (!isUpdateDir) {
                            NavigableMap<String, SongData> tail = songTbMap.tailMap(dirKey, true);
                            Iterator<String> tit = tail.keySet().iterator();
                            while (tit.hasNext()) {
                                String key = tit.next();
                                if (!key.startsWith(dirKey)) break;
                                tit.remove();
                            }
                            continue;
                        }

                        // 更新ありフォルダケース: 各BMSを処理
                        for (EverythingSearchResult sr : bmsFiles) {
                            Path bmsPath = sr.path();
                            final String pathname = (bmsPath.startsWith(root) ? root.relativize(bmsPath).toString() : bmsPath.toString());
                            long bmsModTime;
                            bmsModTime = sr.lastModified();

                            // 対象BMSを処理済みにして、songのDELETE対象から外す
                            final SongData songDataFromTb = songTbMap.remove(pathname);
                            // 既存BMS(songテーブルのレコードとフルパス名と更新日時が一致)の場合はpreview音源のみ更新処理する
                            if (songDataFromTb != null && songDataFromTb.getDate() == bmsModTime) {
                                final String oldpp = songDataFromTb.getPreview() == null ? "" : songDataFromTb.getPreview();
                                final String newpp = previewFileName == null ? "" : previewFileName;
                                // DBのpreviewとフォルダ内のpreviewが一致していない、かつ、フォルダ内のpreviewが空じゃない場合は、フォルダ内のpreviewをsongにUPDATEする
                                // 単に一致しない場合新しいものにすると、#PREVIEW _preview.wavのように指定されていた時に、音源が消えてしまうことになるので実在する場合のみ更新
                                if (!oldpp.equals(newpp) && !newpp.isEmpty()) {
                                    try {
                                        int updated = qr.update(conn, "UPDATE song SET preview=? WHERE path = ?", newpp, pathname);
                                        if (updated > 0) songUpdateCount.addAndGet(updated);
                                    } catch (SQLException e) {
                                        Logger.getGlobal().warning("Error while updating preview at " + pathname + ": " + e.getMessage());
                                    }
                                }
                                continue; // 既存BMSはpreview音源のみ更新して次のBMSの処理へ
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
                                Logger.getGlobal().log(Level.SEVERE,"Error while decoding " + pathname + ": " + e.getMessage(), e);
                            }
                            if (model == null) continue; // デコードできなかったらcontinue

                            SongData sd;
                            try {
                                sd = new SongData(model, hasTxt);
                            } catch (Throwable t) {
                                Logger.getGlobal().log(Level.SEVERE,"SongData 生成失敗 : path=" + pathname + " cause=" + t.getMessage(), t);
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
                                if ((sd.getPreview() == null || sd.getPreview().isEmpty()) && previewFileName != null) {
                                    sd.setPreview(previewFileName);
                                }

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
                                    psSong.setString(8, "");
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
                                    psSong.setInt(25, (int) bmsModTime);
                                    psSong.setInt(26, 0);
                                    psSong.setInt(27, (int) updatetime);
                                    psSong.setInt(28, sd.getNotes());
                                    psSong.setString(29, sd.getCharthash());

                                    psSong.addBatch();
                                    int cur = pendingSong.incrementAndGet();
                                    if (cur >= BATCH_SIZE) {
                                        flushSong.run();
                                    }
                                } catch (SQLException e) {
                                    Logger.getGlobal().log(Level.SEVERE,"song バッチ用パラメータ設定中の例外: " ,e);
                                }

                                // songinfo.dbのUPDATE
                                if (info != null) info.update(model);

                                newBmsCount.incrementAndGet();
                            } else {
                                // ノーツ0かつwav0 -> 削除 (そもそもDBに登録されていないはず？)
                                try {
                                    int deleted = qr.update(conn, "DELETE FROM song WHERE path = ?", pathname);
                                    if (deleted > 0) songDeleteCount.addAndGet(deleted);
                                } catch (SQLException e) { Logger.getGlobal().log(Level.SEVERE, "songレコード削除の例外", e); }
                            }
                        } // for bmsFiles
                    } // end scanFolders

                    // 走査後: 残っているバッチを flush
                    flushSong.run();
                    flushFolder.run();

                    // 走査後、songTbMap に残っているものは実ファイルが存在しないレコードなので削除
                    for (SongData leftover : songTbMap.values()) {
                        try {
                            int deleted = qr.update(conn, "DELETE FROM song WHERE path = ?", leftover.getPath());
                            if (deleted > 0) songDeleteCount.addAndGet(deleted);
                        } catch (SQLException e) {
                            Logger.getGlobal().log(Level.SEVERE, "ディレクトリ内に存在しないsongレコード削除の例外", e);
                        }
                    }

                    // 同様に、folderTbMap に残っているものは実フォルダが存在しないレコードなので削除
                    for (FolderData leftover : folderTbMap.values()) {
                        try {
                            int fd = qr.update(conn, "DELETE FROM folder WHERE path LIKE ?", leftover.getPath() + "%");
                            if (fd > 0) folderDeleteCount.addAndGet(fd);
                            int sd = qr.update(conn, "DELETE FROM song WHERE path LIKE ?", leftover.getPath() + "%");
                            if (sd > 0) songDeleteCount.addAndGet(sd);
                        } catch (SQLException e) {
                            Logger.getGlobal().log(Level.SEVERE, "ディレクトリ内に存在しないfolderレコード削除の例外:", e);
                        }
                    }
                    // commit は外側で行う（try-with-resources の conn を使用）
                } // end try-with-resources psSong/psFolder

                // 楽曲のタグ,FAVORITEのを一時テーブルから復元
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの復元を開始します");
                st.executeUpdate(
                "UPDATE song SET " +
                    "tag = COALESCE((SELECT tag FROM temp_tags WHERE temp_tags.sha256 = song.sha256), song.tag), " +
                    "favorite = COALESCE((SELECT favorite FROM temp_tags WHERE temp_tags.sha256 = song.sha256), song.favorite) " +
                    "WHERE EXISTS (SELECT 1 FROM temp_tags WHERE temp_tags.sha256 = song.sha256);"
                );
                st.executeUpdate("DROP TABLE IF EXISTS temp_tags;");
                Logger.getGlobal().info("楽曲のタグ,FAVORITEの復元が完了しました");

                conn.commit();
            } catch (Exception e) {
                Logger.getGlobal().log(Level.SEVERE, "楽曲データベース更新時の例外", e);
            } // songdata.db用トランザクション終了 try-with-resources

            if(info != null) {
                // songinfo.db用トランザクション終了
                info.endUpdate();
            }
            long nowtime = System.currentTimeMillis();
            Logger.getGlobal().info("楽曲更新完了 : Time - " + (nowtime - starttime) + " 1曲あたりの時間 - "
                    + (newBmsCount.get() > 0 ? (nowtime - starttime) / newBmsCount.get() : "不明"));
            Logger.getGlobal().info("DB 操作件数: song insert=" + songInsertCount.get() + " update=" + songUpdateCount.get() + " delete=" + songDeleteCount.get()
                    + " | folder insert=" + folderInsertCount.get() + " delete=" + folderDeleteCount.get());
        }
	}

//	private static class SongDatabaseUpdaterProperty {
//		private final Map<String, String> tags = new HashMap<String, String>();
//		private final Map<String, Integer> favorites = new HashMap<String, Integer>();
//		private final SongInformationAccessor info;
//		private final long updatetime;
//		private final AtomicInteger count = new AtomicInteger();
//		private Connection conn;
//
//		public SongDatabaseUpdaterProperty(long updatetime, SongInformationAccessor info) {
//			this.updatetime = updatetime;
//			this.info = info;
//		}
//	}
//
//	public static interface SongDatabaseAccessorPlugin {
//		public void update(BMSModel model, SongData song);
//	}
}
