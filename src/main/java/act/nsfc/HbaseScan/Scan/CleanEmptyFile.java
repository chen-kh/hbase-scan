package act.nsfc.HbaseScan.Scan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class CleanEmptyFile {

	public static void main(String[] args) {
		String dirpath = "C:\\Users\\LanTian\\workspace\\HbaseScan\\data";
		// TODO Auto-generated method stub
		try {
			File filedir = new File(dirpath);
			String[] datefiledir = filedir.list();
			for (int i = 0; i < datefiledir.length; i++) {
				File file = new File(dirpath + datefiledir[i]);
				String[] datafile = file.list();
				for (int j = 0; j < datafile.length; j++) {
					File file2 = new File(dirpath + datefiledir[i] + "\\" + datafile[j]);
					FileReader fileReader = new FileReader(file2);
					BufferedReader br = new BufferedReader(fileReader);
					String str = br.readLine();
					str = br.readLine();
					fileReader.close();
					br.close();
					if (str == null) {
						file2.delete();
					}					
				}
			}
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
	}

}
