package burkemw3.vanek;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.imageio.ImageIO;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class Startup {
    private static class CliOptions {
        static final String DIRECTORY = "directory";
        static final String BUCKET_NAME = "bucket-name";
        static final String ALBUM_NAME = "album-name";
        static final String FORCE_UPLOAD = "force-upload";
        static final String HELP = "help";
    }
    private static final String PicturesPrefix = "pictures";

    private static TransferManager _transferManager;
    private static Collection<Upload> _uploads = new ArrayList<Upload>();

    public static void main(String[] arguments) throws IOException,
            AmazonServiceException, AmazonClientException, InterruptedException {
        AmazonS3 s3 = connectToS3();

        Options options = new Options();
        options.addOption("b", CliOptions.BUCKET_NAME, true, "bucket name");
        options.addOption("a", CliOptions.ALBUM_NAME, true, "album name");
        options.addOption("f", CliOptions.FORCE_UPLOAD, false,
                "force upload, overwrite existing");
        options.addOption("d", CliOptions.DIRECTORY, true, "directory");
        options.addOption("h", CliOptions.HELP, false, "help");

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine line = parser.parse(options, arguments);

            if (true == line.hasOption(CliOptions.HELP)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("vanek", options);
                return;
            }

            String bucketName = line.getOptionValue(CliOptions.BUCKET_NAME);
            if (bucketName == null) {
                throw new ParseException("Bucket name must be specified");
            }

            String albumName = line.getOptionValue(CliOptions.ALBUM_NAME);
            if (albumName == null) {
                throw new ParseException("Album name must be specified");
            } else if (albumName.matches("[^A-Za-z0-9_-]")) {
                throw new ParseException(
                        "Album name can only contain numbers, letters, hyphens, and underscores");
            }

            String directory = line.getOptionValue(CliOptions.DIRECTORY);
            Collection<File> files = getJpegsInDirectory(directory);

            ensureBucketExists(s3, bucketName);

            boolean forceUpdate = line.hasOption(CliOptions.FORCE_UPLOAD);
            String zipKeyName = createAndUploadZipFile(s3, bucketName, albumName, files, forceUpdate);
            createAndUploadGalleryImages(s3, bucketName, albumName, files);
            String galleryKeyName = createAndUploadGallery(s3, bucketName, albumName, zipKeyName, files);

            System.out.print("Waiting for uploads to finish...");
            for (Upload upload : _uploads) {
                upload.waitForCompletion();
            }
            System.out.println("done.");

            generateSharingLink(bucketName, galleryKeyName);
        } catch (ParseException exp) {
            System.err.println("Parsing failed. Reason: " + exp.getMessage());
        } catch (Exception e) {
            System.err.println("Unhandled exception: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (_transferManager != null) {
                _transferManager.shutdownNow();
            }
        }
    }

    private static void createAndUploadGalleryImages(AmazonS3 s3, String bucketName, String albumName,
            Collection<File> files) throws IOException, InterruptedException {
        System.out.print("Resizing images...");
        for (File fullsizeImage : files) {
            fullsizeImage.deleteOnExit();
            File galleryImage = resizeImage(fullsizeImage, Dimension.ANY, 2048);
            String galleryImageKeyName = String.format("%s/%s/images/%s.jpg", PicturesPrefix, albumName,
                    fullsizeImage.getName());
            ObjectMetadata galleryImageMetadata = new ObjectMetadata();
            galleryImageMetadata.setContentType("image/jpeg");
            uploadFile(s3, bucketName, galleryImage, galleryImageKeyName, galleryImageMetadata);

            File thumbnailImage = resizeImage(fullsizeImage, Dimension.HEIGHT, 66);
            String thumbnailImageKeyName = String.format("%s/%s/thumbnails/%s.jpg", PicturesPrefix,
                    albumName, fullsizeImage.getName());
            ObjectMetadata thumbnailImageMetadata = new ObjectMetadata();
            thumbnailImageMetadata.setContentType("image/jpeg");
            uploadFile(s3, bucketName, thumbnailImage, thumbnailImageKeyName, thumbnailImageMetadata);
        }
        System.out.println("done.");
    }

    private static enum Dimension {
        HEIGHT,
        ANY,
    }

    private static File resizeImage(File fullsizeImage, Dimension dimension, int size) throws IOException {
        BufferedImage src = ImageIO.read(fullsizeImage);

        // figure out width and height
        int width, height;
        switch(dimension) {
        case HEIGHT:
            height = size;
            width = (int) (((double)src.getWidth())/src.getHeight() * height);
            break;
        case ANY:
            if (src.getHeight() > src.getWidth()) {
                height = size;
                width = (int) (((double)src.getWidth())/src.getHeight() * height);
            } else {
                width = size;
                height = (int) (((double)src.getHeight())/src.getWidth() * width);
            }
            break;
        default:
            throw new RuntimeException("I don't know what " + dimension.toString() + " is");
        }

        BufferedImage destination = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics = destination.createGraphics();
        graphics.drawImage(src, 0, 0, width, height, null);

        File outputFile = File.createTempFile("images", ".jpg");
        outputFile.deleteOnExit();
        ImageIO.write(destination, "jpeg", outputFile);
        src.flush();
        destination.flush();
        return outputFile;
    }

    private static final String _minifiedTemplate =
            "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01//EN\" \"http://www.w3.org/TR/html4/strict.dtd\">" +
            "<html><head><title>%s</title><link rel=\"stylesheet\" type=\"text/css\" href=\"//s3.amazonaws.com/burkemw3/ad-gallery/jquery.ad-gallery.css\"><style type=\"text/css\">body{background-color:Black;}#download_link{position:absolute;top:8px;right:8px;z-index:1;}#gallery{padding-left:5%%;padding-right:5%%;width:90%%;}</style></head><body><div id=\"container\"><div id=\"gallery\" class=\"ad-gallery\"><div class=\"ad-image-wrapper\"></div><div class=\"ad-nav\"><div class=\"ad-thumbs\"><ul class=\"ad-thumb-list\">%s</ul></div></div></div></div><script type=\"text/javascript\" src=\"//cdnjs.cloudflare.com/ajax/libs/jquery/1.7.2/jquery.min.js\"></script><script type=\"text/javascript\" src=\"//s3.amazonaws.com/burkemw3/ad-gallery/jquery.ad-gallery.min.js\"></script><script type=\"text/javascript\">$(function(){window.galleries=$('.ad-gallery').adGallery({height:($(window).height()-100-25),effect:\"none\",loader_image:\"//s3.amazonaws.com/burkemw3/ad-gallery/loader.gif\"});var downloadUrl='%s';$.ajax({url:downloadUrl,type:'HEAD',success:function(){$('body').prepend('<div id=\"download_link\"><a href=\"'+downloadUrl+'\">Download images</a></div>');}});});</script></body></html>";
    private static final String _imageTemplate = "<li><a href=\"images/%s.jpg\"><img src=\"thumbnails/%s.jpg\" class=\"class0\" /></a></li>";

    private static String createAndUploadGallery(AmazonS3 s3, String bucketName, String albumName,
            String zipKeyName, Collection<File> files) throws IOException, InterruptedException {
        StringBuilder imageHtml = new StringBuilder();
        for (File file : files) {
            imageHtml.append(String.format(_imageTemplate, file.getName(), file.getName()));
        }
        String zipfileUrl = getAmazonUrl(bucketName, zipKeyName);
        String html = String.format(_minifiedTemplate, albumName, imageHtml, zipfileUrl);
        File file = File.createTempFile("gallery", ".html");
        file.deleteOnExit();
        FileWriter writer = new FileWriter(file);
        writer.write(html.toString());
        writer.close();
        String galleryKeyName = String.format("%s/%s/index.html", PicturesPrefix, albumName);
        uploadFile(s3, bucketName, file, galleryKeyName, null);

        return galleryKeyName;
    }

    private static String getAmazonUrl(String bucketName, String keyName) {
        return String.format("http://s3.amazonaws.com/%s/%s", bucketName, keyName);
    }

    private static AmazonS3 connectToS3() throws FileNotFoundException, IOException {
        /* http://aws.amazon.com/security-credentials */
        String homeDirectoryPath = System.getProperty("user.home");
        String awsPropertiesPath = "AwsCredentials.properties";
        File awsCredentials = new File(
                homeDirectoryPath + "/" + awsPropertiesPath);
        FileInputStream fi = new FileInputStream(awsCredentials);
        AmazonS3 s3 = new AmazonS3Client(new PropertiesCredentials(fi));
        _transferManager = new TransferManager(s3);
        return s3;
    }

    private static void ensureBucketExists(AmazonS3 s3, String bucketName) {
        s3.createBucket(bucketName);
    }

    public static class FilenameComparator implements Comparator<File>{
        @Override
        public int compare(File left, File right) {
            return (left.getName().compareTo(right.getName()));
        }
    }

    private static Collection<File> getJpegsInDirectory(String path) {
        File root = new File(path);
        List<File> files = new ArrayList<File>();

        for (File file : root.listFiles()) {
            if (false == file.isFile()) {
                continue;
            }
            String filename = file.getName();
            String fileExtension = filename.substring(
                    filename.length() - 3, filename.length());
            if (false == fileExtension.equalsIgnoreCase("jpg")) {
                continue;
            }

            files.add(file);
        }

        Collections.sort(files, new FilenameComparator());

        return files;
    }

    public static void uploadFile(AmazonS3 s3, String bucketName, File file, String keyName,
            ObjectMetadata metadata) throws InterruptedException {
        PutObjectRequest put = new PutObjectRequest(bucketName, keyName, file);

        put.setStorageClass(StorageClass.ReducedRedundancy);
        put.withCannedAcl(CannedAccessControlList.PublicRead);

        if (metadata != null) {
            put.setMetadata(metadata);
        }

        Upload upload = _transferManager.upload(put);
        _uploads.add(upload);
    }

    private static String createAndUploadZipFile(AmazonS3 s3, String bucketName,
            String albumName, Collection<File> files, boolean forceUpdate)
            throws IOException, AmazonServiceException, AmazonClientException,
            InterruptedException {
        if (false == forceUpdate) {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(bucketName);
            ObjectListing objectListing = s3.listObjects(listObjectsRequest);
            List<S3ObjectSummary> objectSummaries =
                    objectListing.getObjectSummaries();
            for (S3ObjectSummary objectSummary : objectSummaries) {
                if (objectSummary.getKey().startsWith(albumName)) {
                    throw new IOException("file exists on s3. you can force an upload though");
                }
            }
        }

        File zipFile = createZipFile(files);
        String zipKeyName = String.format("%s/%s/%s.zip", PicturesPrefix, albumName, albumName);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/zip");
        uploadFile(s3, bucketName, zipFile, zipKeyName, metadata);

        return zipKeyName;
    }

    static final int ZIP_BUFFER_SIZE = 2048;

    private static File createZipFile(Collection<File> files) throws IOException {
        File file = File.createTempFile("images", ".zip");
        file.deleteOnExit();
        FileOutputStream dest = new FileOutputStream(file);
        BufferedOutputStream outStream = new BufferedOutputStream(dest);
        ZipOutputStream out = new ZipOutputStream(outStream);
        byte data[] = new byte[ZIP_BUFFER_SIZE];
        int total = files.size();
        System.out.format("Zipping %d files...", total);
        for (File inputFile : files) {
            FileInputStream fi = new FileInputStream(inputFile);
            BufferedInputStream origin =
                    new BufferedInputStream(fi, ZIP_BUFFER_SIZE);
            ZipEntry entry = new ZipEntry(inputFile.getName());
            out.putNextEntry(entry);
            int count;
            while ((count = origin.read(data, 0, ZIP_BUFFER_SIZE)) != -1) {
                out.write(data, 0, count);
            }
            origin.close();
        }
        out.close();
        System.out.println("done");

        return file;
    }

    private static void generateSharingLink(String bucketName, String galleryKeyName) {
        System.out.println();
        String galleryUrl = getAmazonUrl(bucketName, galleryKeyName);
        System.out.format("You can view and download the pictures at %s.\n", galleryUrl);
    }
}
