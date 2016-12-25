package com.zmyuan.demo.socket;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class TransferToClient {
	
	public static void main(String[] args) throws IOException{
		TransferToClient sfc = new TransferToClient();
		sfc.testSendfile();
	}
	public void testSendfile() throws IOException {
	    String host = "localhost";
	    int port = 9026;
	    SocketAddress sad = new InetSocketAddress(host, port);
	    SocketChannel sc = SocketChannel.open();
	    sc.connect(sad);
	    sc.configureBlocking(true);

//	    String fname = "E:\\downloads\\迅雷下载\\生死狙击DVD修正版CD2[www.dygod.com电影天堂].rmvb";
	    String fname = "E:\\downloads\\迅雷下载\\eclipse-inst-win64.exe";
	    long fsize = 1836783750000000000L, sendzise = 4094;
	    
	    // FileProposerExample.stuffFile(fname, fsize);
	    FileChannel fc = new FileInputStream(fname).getChannel();
            long start = System.currentTimeMillis();
	    long nsent = 0, curnset = 0;
	    curnset =  fc.transferTo(0, fsize, sc);
	    System.out.println("传输字节数--"+curnset+" 耗时毫秒数 --"+(System.currentTimeMillis() - start));
	    //fc.close();
	  }


}
