/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jsontomongo;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author mayote
 */
public class JSONtoMongo {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        try {
            walkin(new File("/home/mayote/NetBeansProjects/TwitterDataAnalytics/streaming/"));
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }
    }

    public static void moveFile(File sourceFile, File destFile) throws IOException {
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        FileChannel origen = null;
        FileChannel destino = null;
        try {
            origen = new FileInputStream(sourceFile).getChannel();
            destino = new FileOutputStream(destFile).getChannel();

            long count = 0;
            long size = origen.size();
            while ((count += destino.transferFrom(origen, count, size - count)) < size);
        } finally {
            if (origen != null) {
                origen.close();
            }
            if (destino != null) {
                destino.close();
            }
        }
        if (sourceFile.delete())
        {
            System.out.println("Respaldado: "+sourceFile.getName());
        }
        else
        {
            System.out.println("No se respaldó: "+sourceFile.getName());
        }
    }

    public static void walkin(File dir) throws IOException {
        String pattern = ".json";

        File listFile[] = dir.listFiles();
        System.out.println(listFile.length);
        if (listFile != null) {
            for (int i = 0; i < listFile.length; i++) {
                if (listFile[i].isDirectory()) {
                    //walkin(listFile[i]);
                } else {
                    if (listFile[i].getName().endsWith(pattern)) {
                        System.out.println(listFile[i].getPath());
                        //System.out.println(listFile[i].getName());
                        ejecutaComando(listFile[i].getPath());
                        moveFile(listFile[i], new File("/home/mayote/NetBeansProjects/TwitterDataAnalytics/streaming/procesados/"+listFile[i].getName()));
                    }
                }
            }
        }
    }

    public static void ejecutaComando(String archivo) {
        // TODO code application logic here
        String s = null;

        try {
            String comando;

            comando = "mongoimport --db twitter "
                    + "--collection tweets "
                    + "--type json "
                    + "--file " + archivo;

            // Ejcutamos el comando
            Process p = Runtime.getRuntime().exec(comando);

            BufferedReader stdInput = new BufferedReader(new InputStreamReader(
                    p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new InputStreamReader(
                    p.getErrorStream()));
            // Leemos la salida del comando
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }

        } catch (IOException e) {
            System.out.println("Excepción: ");
            e.printStackTrace();
            //System.exit(-1);
        }
    }

}
