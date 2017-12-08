package com.upplication.s3fs.util;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;


public class CopyDirVisitor extends SimpleFileVisitor<Path> {

    private Path fromPath;
    private Path toPath;
    private StandardCopyOption copyOption;

    public CopyDirVisitor(Path fromPath, Path toPath, StandardCopyOption copyOption) {
        this.fromPath = fromPath;
        this.toPath = toPath;
        this.copyOption = copyOption;
    }

    public CopyDirVisitor(Path fromPath, Path toPath) {
        this(fromPath, toPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

        // we allow to work against different providers
        Path targetPath = appendPath(dir);

        if (!Files.exists(targetPath)) {
            if (!targetPath.toString().endsWith("/")) {
                targetPath = targetPath.getParent().resolve(targetPath.getFileName().toString() + "/");
            }
            Files.createDirectory(targetPath);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

        Path targetPath = appendPath(file);

        Files.copy(file, targetPath, copyOption);
        return FileVisitResult.CONTINUE;
    }

    /**
     * Obtenemos el path que corresponde en el parametro: {@link #fromPath}
     * relativo al parametro <code>Path to</code>
     *
     * @param to Path
     * @return
     */
    private Path appendPath(Path to) {
        Path targetPath = toPath;
        // sacamos el path relativo y lo recorremos para
        // añadirlo al nuevo
        for (Path path : fromPath.relativize(to)) {
            // si utilizamos path en vez de string: lanza error por ser
            // distintos paths
            targetPath = targetPath.resolve(path.getFileName().toString());
        }
        return targetPath;
    }
}