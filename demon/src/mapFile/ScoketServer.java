package mapFile;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;



public class ScoketServer {
	public static void main(String[] args) throws Exception {
		ServerSocket server = new ServerSocket(9090);
		Socket socket  = null;
		while(true){
			socket = server.accept();
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			bw.write("abc");
			//bw.write("<br>");
			/*bw.write("def");
			bw.write("<br>");*/
			bw.write("中");
			bw.close();
		}
	}
}
