package br.com.assemblenewtechnologies.ANTLogSync.Helpers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipUtils {
	private static Logger LOGGER = LoggerFactory.getLogger(ZipUtils.class);
	private List<String> fileList;
	private String OUTPUT_ZIP_FILE = null; // "Folder.zip";
	private String SOURCE_FOLDER = null; // "D:\\Reports"; // SourceFolder path

	public ZipUtils() {
		fileList = new ArrayList<String>();
	}

	public void zipIt(String zipFile) throws Exception {
		byte[] buffer = new byte[1024];
		String source = new File(SOURCE_FOLDER).getName();
		LOGGER.info("archive SOURCE FOLDER: " + source);
		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		try {
			fos = new FileOutputStream(zipFile);
			zos = new ZipOutputStream(fos);

			LOGGER.info("Archiving to output to Zip : " + zipFile);
			FileInputStream in = null;

			for (String file : this.fileList) {
//				LOGGER.info("Archiving File Added : " + file);
				ZipEntry ze = new ZipEntry(source + File.separator + file);
				zos.putNextEntry(ze);
				try {
					in = new FileInputStream(SOURCE_FOLDER + File.separator + file);
					int len;
					while ((len = in.read(buffer)) > 0) {
						zos.write(buffer, 0, len);
					}
				} finally {
					in.close();
					File archive_file_buffer = new File(source + File.separator + file);
					archive_file_buffer.delete();
				}
			}

			zos.closeEntry();
			LOGGER.info("Folder: "+source+" successfully compressed and archived");

		} catch (IOException ex) {
//			ex.printStackTrace();
			LOGGER.error("Archiving error: " + source);
			LOGGER.error(ex.getMessage());
			throw new Exception (ex);
		} finally {
			try {
				zos.close();
				File dir_buffer = new File(source);
				dir_buffer.delete();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
				throw new Exception (e);
			}
		}
	}

	public void generateFileList(File node) {
		// add file only
		if (node.isFile()) {
			fileList.add(generateZipEntry(node.toString()));
		}

		if (node.isDirectory()) {
			String[] subNote = node.list();
			for (String filename : subNote) {
				generateFileList(new File(node, filename));
			}
		}
	}

	private String generateZipEntry(String file) {
		return file.substring(SOURCE_FOLDER.length() + 1, file.length());
	}

	/**
	 * @return the oUTPUT_ZIP_FILE
	 */
	public String getOUTPUT_ZIP_FILE() {
		return OUTPUT_ZIP_FILE;
	}

	/**
	 * @param oUTPUT_ZIP_FILE the oUTPUT_ZIP_FILE to set
	 */
	public void setOUTPUT_ZIP_FILE(String oUTPUT_ZIP_FILE) {
		OUTPUT_ZIP_FILE = oUTPUT_ZIP_FILE;
	}

	/**
	 * @return the sOURCE_FOLDER
	 */
	public String getSOURCE_FOLDER() {
		return SOURCE_FOLDER;
	}

	/**
	 * @param sOURCE_FOLDER the sOURCE_FOLDER to set
	 */
	public void setSOURCE_FOLDER(String sOURCE_FOLDER) {
		SOURCE_FOLDER = sOURCE_FOLDER;
	}

}