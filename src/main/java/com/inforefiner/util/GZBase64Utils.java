package com.inforefiner.util;

import org.apache.commons.io.IOUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZBase64Utils {

  public static final String DEFAULT_ENCODING = "UTF-8";

  /**
   * 解码
   **/
  public static String uncompressString(String zippedBase64Str) throws IOException {
    char c = zippedBase64Str.charAt(0);
    String result = "";
    if(c == '{') {
      result = zippedBase64Str;
    } else if(c == 'H'){
      byte[] bytes = Base64.getDecoder().decode(zippedBase64Str);
      GZIPInputStream zi = null;
       try {
        zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
        result = IOUtils.toString(zi, DEFAULT_ENCODING);
      } finally {
        IOUtils.closeQuietly(zi);
      }
    }
      return result;
  }

  /**
   * 加码
   * */
  public static String compressString(String srcTxt)
          throws IOException {
    ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
    GZIPOutputStream zos = new GZIPOutputStream(rstBao);
    zos.write(srcTxt.getBytes(DEFAULT_ENCODING));
    IOUtils.closeQuietly(zos);
    byte[] bytes = rstBao.toByteArray();
    return Base64.getEncoder().encodeToString(bytes);
  }


  public static void main(String[] args) {
    try {
      GZBase64Utils gz = new GZBase64Utils();
      String t = gz.uncompressString("asd");
      String t1 = gz.compressString("{\"quoteChar\":\"\\\"\",\"path\":\"/temp/test/aaaa.csv\",\"escapeChar\":\"\\\\\",\"format\":\"parquet\",\"separator\":\",\"}");
      System.out.println(t);
      System.out.println(t1);
    } catch (Exception e){
      e.printStackTrace();
    }
  }

}
