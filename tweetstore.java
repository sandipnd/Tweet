package rocksdb;


import java.lang.IllegalArgumentException;
import java.nio.ByteBuffer;

import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

public class tweetstore {
	
  static Options options = new Options();
  final static String db_path = "/Users/sandipnandi/rocksdb/mydb";
  static tweetstore instance = null;
  static RocksDB db = null;
  
  
  static {
    RocksDB.loadLibrary();
  }
  
  public static byte[] getTimetoByte(long dateInSec) {
	  return ByteBuffer.allocate(16).putLong(dateInSec).array();
  }
  
  public static long getBytetoTime(byte[] bytes ) {
	  return ByteBuffer.wrap(bytes).getLong();
  }

  private tweetstore() {}
  
  public static synchronized tweetstore getInstance(){
      if(instance == null){
          instance = new tweetstore();
          instance.setOption();
          try {
			db = RocksDB.open(options, db_path);
		} catch (RocksDBException e) {
			e.printStackTrace();
		}
      }
      return instance;
  }
  
  public void insert(String username, String tweets ) {
	  try {
	       String key  = username + "_" + Long.toString(System.currentTimeMillis());
    	   db.put(key.getBytes(), tweets.getBytes()); 
	      } catch (final RocksDBException e) {
	        System.err.println(e);
	      }
	 }
  
  public void display(String user) {
	System.out.print(user);
	try (final RocksIterator iterator = db.newIterator())  {
			   iterator.seek(user.getBytes());
			   while ( iterator.isValid()) {
				   System.out.println(new String(iterator.value()));   
				   iterator.next();
			}
		
	}
  }
  
  public void delete() {
	  try (final RocksIterator iterator = db.newIterator()) {
		   for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
			   try {
				db.delete(iterator.key());
			} catch (RocksDBException e) {
				e.printStackTrace();
			}
		   }
	   }
  }
  
  public void setOption() {
	  
	    final String db_path_not_found = db_path + "_not_found";
	    System.out.println("RocksDBSample");
	    try (
	         final Filter bloomFilter = new BloomFilter(10);
	         final ReadOptions readOptions = new ReadOptions()
	             .setFillCache(false);
	         final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10)) {

	      try (final RocksDB db = RocksDB.open(options, db_path_not_found)) {
	        assert (false);
	      } catch (final RocksDBException e) {
	        System.out.format("Caught the expected exception -- %s\n", e);
	      }

	      try {
	        options.setCreateIfMissing(true)
	            .createStatistics()
	            .setWriteBufferSize(8 * SizeUnit.KB)
	            .setMaxWriteBufferNumber(3)
	            .setMaxBackgroundCompactions(10)
	            .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
	            .setCompactionStyle(CompactionStyle.UNIVERSAL);
	      } catch (final IllegalArgumentException e) {
	        assert (false);
	      }

	      final Statistics stats = options.statisticsPtr();

	      assert (options.createIfMissing() == true);
	      assert (options.writeBufferSize() == 8 * SizeUnit.KB);
	      assert (options.maxWriteBufferNumber() == 3);
	      assert (options.maxBackgroundCompactions() == 10);
	      assert (options.compressionType() == CompressionType.SNAPPY_COMPRESSION);
	      assert (options.compactionStyle() == CompactionStyle.UNIVERSAL);

	      assert (options.memTableFactoryName().equals("SkipListFactory"));
	      options.setMemTableConfig(
	          new HashSkipListMemTableConfig()
	              .setHeight(4)
	              .setBranchingFactor(4)
	              .setBucketCount(2000000));
	      assert (options.memTableFactoryName().equals("HashSkipListRepFactory"));

	      options.setMemTableConfig(
	          new HashLinkedListMemTableConfig()
	              .setBucketCount(100000));
	      assert (options.memTableFactoryName().equals("HashLinkedListRepFactory"));

	      options.setMemTableConfig(
	          new VectorMemTableConfig().setReservedSize(10000));
	      assert (options.memTableFactoryName().equals("VectorRepFactory"));

	      options.setMemTableConfig(new SkipListMemTableConfig());
	      
	      assert (options.memTableFactoryName().equals("SkipListFactory"));

	      options.setTableFormatConfig(new PlainTableConfig());
	      // Plain-Table requires mmap read
	      options.setAllowMmapReads(true);
	      assert (options.tableFactoryName().equals("PlainTable"));

	      options.setRateLimiter(rateLimiter);
	      options.useFixedLengthPrefixExtractor(8);

	      final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
	      table_options.setBlockCacheSize(64 * SizeUnit.KB)
	          .setFilter(bloomFilter)
	          .setCacheNumShardBits(6)
	          .setBlockSizeDeviation(5)
	          .setBlockRestartInterval(10)
	          .setCacheIndexAndFilterBlocks(true)
	          .setHashIndexAllowCollision(false)
	          .setBlockCacheCompressedSize(64 * SizeUnit.KB)
	          .setBlockCacheCompressedNumShardBits(10);

	      assert (table_options.blockCacheSize() == 64 * SizeUnit.KB);
	      assert (table_options.cacheNumShardBits() == 6);
	      assert (table_options.blockSizeDeviation() == 5);
	      assert (table_options.blockRestartInterval() == 10);
	      assert (table_options.cacheIndexAndFilterBlocks() == true);
	      assert (table_options.hashIndexAllowCollision() == false);
	      assert (table_options.blockCacheCompressedSize() == 64 * SizeUnit.KB);
	      assert (table_options.blockCacheCompressedNumShardBits() == 10);

	      options.setTableFormatConfig(table_options);
	      assert (options.tableFactoryName().equals("BlockBasedTable"));

	      try (final RocksDB db = RocksDB.open(options, db_path)) {
	        db.put("hello".getBytes(), "world".getBytes());

	        final byte[] value = db.get("hello".getBytes());
	        assert ("world".equals(new String(value)));

	        final String str = db.getProperty("rocksdb.stats");
	        assert (str != null && !str.equals(""));
	      } catch (final RocksDBException e) {
	        System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
	        assert (false);
	      }
	      } 
	    
  }
  
 
  public void mytest() {
   

    final String db_path = "/Users/sandipnandi/rocksdb/mydb";
    final String db_path_not_found = db_path + "_not_found";
    
    System.out.println("RocksDBSample");
    try (final Options options = new Options();
         final Filter bloomFilter = new BloomFilter(10);
         final ReadOptions readOptions = new ReadOptions()
             .setFillCache(false);
         final RateLimiter rateLimiter = new RateLimiter(10000000,10000, 10)) {

      try (final RocksDB db = RocksDB.open(options, db_path_not_found)) {
        assert (false);
      } catch (final RocksDBException e) {
        System.out.format("Caught the expected exception -- %s\n", e);
      }

      try {
        options.setCreateIfMissing(true)
            .createStatistics()
            .setWriteBufferSize(8 * SizeUnit.KB)
            .setMaxWriteBufferNumber(3)
            .setMaxBackgroundCompactions(10)
            .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
            .setCompactionStyle(CompactionStyle.UNIVERSAL);
      } catch (final IllegalArgumentException e) {
        assert (false);
      }

      final Statistics stats = options.statisticsPtr();

      assert (options.createIfMissing() == true);
      assert (options.writeBufferSize() == 8 * SizeUnit.KB);
      assert (options.maxWriteBufferNumber() == 3);
      assert (options.maxBackgroundCompactions() == 10);
      assert (options.compressionType() == CompressionType.SNAPPY_COMPRESSION);
      assert (options.compactionStyle() == CompactionStyle.UNIVERSAL);

      assert (options.memTableFactoryName().equals("SkipListFactory"));
      options.setMemTableConfig(
          new HashSkipListMemTableConfig()
              .setHeight(4)
              .setBranchingFactor(4)
              .setBucketCount(2000000));
      assert (options.memTableFactoryName().equals("HashSkipListRepFactory"));

      options.setMemTableConfig(
          new HashLinkedListMemTableConfig()
              .setBucketCount(100000));
      assert (options.memTableFactoryName().equals("HashLinkedListRepFactory"));

      options.setMemTableConfig(
          new VectorMemTableConfig().setReservedSize(10000));
      assert (options.memTableFactoryName().equals("VectorRepFactory"));

      options.setMemTableConfig(new SkipListMemTableConfig());
      
      assert (options.memTableFactoryName().equals("SkipListFactory"));

      options.setTableFormatConfig(new PlainTableConfig());
      // Plain-Table requires mmap read
      options.setAllowMmapReads(true);
      assert (options.tableFactoryName().equals("PlainTable"));

      options.setRateLimiter(rateLimiter);
      options.useFixedLengthPrefixExtractor(8);

      final BlockBasedTableConfig table_options = new BlockBasedTableConfig();
      table_options.setBlockCacheSize(64 * SizeUnit.KB)
          .setFilter(bloomFilter)
          .setCacheNumShardBits(6)
          .setBlockSizeDeviation(5)
          .setBlockRestartInterval(10)
          .setCacheIndexAndFilterBlocks(true)
          .setHashIndexAllowCollision(false)
          .setBlockCacheCompressedSize(64 * SizeUnit.KB)
          .setBlockCacheCompressedNumShardBits(10);

      assert (table_options.blockCacheSize() == 64 * SizeUnit.KB);
      assert (table_options.cacheNumShardBits() == 6);
      assert (table_options.blockSizeDeviation() == 5);
      assert (table_options.blockRestartInterval() == 10);
      assert (table_options.cacheIndexAndFilterBlocks() == true);
      assert (table_options.hashIndexAllowCollision() == false);
      assert (table_options.blockCacheCompressedSize() == 64 * SizeUnit.KB);
      assert (table_options.blockCacheCompressedNumShardBits() == 10);

      options.setTableFormatConfig(table_options);
      assert (options.tableFactoryName().equals("BlockBasedTable"));

      try (final RocksDB db = RocksDB.open(options, db_path)) {
        db.put("hello".getBytes(), "world".getBytes());

        final byte[] value = db.get("hello".getBytes());
        assert ("world".equals(new String(value)));

        final String str = db.getProperty("rocksdb.stats");
        assert (str != null && !str.equals(""));
      } catch (final RocksDBException e) {
        System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
        assert (false);
      }

      try (final RocksDB db = RocksDB.open(options, db_path)) {
        /*
    	db.put("hello".getBytes(), "world".getBytes());
        long time = System.currentTimeMillis();
        db.put("drump".getBytes(), "worldlead".getBytes() );
        byte[] value = db.get("drump".getBytes());
        System.out.format("Get('hello') = %s\n",
            new String(value));
        */
       byte[] val = null;
       byte[] value = null;
       for (int i = 0; i < 10; i++) {
    	   val = tweetstore.getTimetoByte(System.currentTimeMillis());
    	   //db.put(val, String.valueOf(i).getBytes());
    	   String v = "14975611528"+String.valueOf(i);
    	   int j = i * 10;
    	   db.put(v.getBytes(), String.valueOf(j).getBytes());
    	   value = db.get(val);
    	   //System.out.format("Get('me') = %s\n",
    	            //new String(value));
       }
       
    
	   
	   
	   byte[]val1 = null;
	   //System.out.println(db.get(val1, val));
	   try (final RocksIterator iterator = db.newIterator())  {
		   iterator.seek("1497561152864".getBytes());
		   while ( iterator.isValid()) {
			   System.out.println(new String(iterator.value()));   
			   iterator.next();
		   }
		   
		   
	   }
	   int count = 0;
	   try (final RocksIterator iterator = db.newIterator()) {
		   for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
			   //System.out.format(" %s ", iterator.key());
			   db.delete(iterator.key());
			   count ++;
		   }
		   System.out.println("keys are " + count);
	   }
      } catch (final RocksDBException e) {
        System.err.println(e);
      }
    }
  }
}
