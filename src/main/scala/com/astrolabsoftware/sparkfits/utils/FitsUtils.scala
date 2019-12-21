package com.astrolabsoftware.sparkfits.utils

import com.astrolabsoftware.sparkfits.FitsLib.Fits
import com.astrolabsoftware.sparkfits.FitsSchema.getSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}

import scala.util.Try

object FitsUtils {
  /**
    * Search for input FITS files. The input path can be either a single
    * FITS file, or a folder containing several FITS files with the
    * same HDU structure, or a globbing structure e.g. "toto/\*.fits".
    * Raise a NullPointerException if no files found.
    *
    * @param fn : (String)
    *   Input path.
    * @return (List[String]) List with all files found.
    *
    */
  def searchFitsFile(fn: String, conf: Configuration, verbosity: Boolean=false): List[String] = {
    // Make it Hadoop readable
    val path = new Path(fn)
    val fs = path.getFileSystem(conf)

    // Check whether we are globbing
    val isGlob : Boolean = Try{fs.globStatus(path).size > 1}.getOrElse(false)

    val isCommaSep : Boolean = Try{fn.split(",").size > 1}.getOrElse(false)

    // Check whether we want to load a single FITS file or several
    val isDir : Boolean = fs.isDirectory(path)
    val isFile : Boolean = fs.isFile(path)

    // println(s"isDir=$isDir isFile=$isFile path=$path")

    // List all the files
    val listOfFitsFiles : List[String] = if (isGlob) {
      val arr = fs.globStatus(path)
      arr.map(x => x.getPath.toString).toList
    } else if (isDir) {
      val it = fs.listFiles(path, true)
      getListOfFiles(it).filter{file => file.endsWith(".fits")}
    } else if (isCommaSep) {
      fn.split(",").toList
    } else if (isFile){
      List(fn)
    } else {
      List[String]()
    }

    // Check that we have at least one file
    listOfFitsFiles.size match {
      case x if x > 0 => if (verbosity) {
        println("FitsRelation.searchFitsFile> Found " + listOfFitsFiles.size.toString + " file(s):")
        listOfFitsFiles.foreach(println)
      }
      case x if x <= 0 => throw new NullPointerException(s"""
          0 files detected! Is $fn a directory containing
          FITS files or a FITS file?
          """)
    }

    listOfFitsFiles
  }

  /**
    * Load recursively all FITS file inside a directory.
    *
    * @param it : (RemoteIterator[LocatedFileStatus])
    *   Iterator from a Hadoop Path containing informations about files.
    * @param extensions : (List[String)
    *   List of accepted extensions. Currently only .fits is available.
    *   Default is List("*.fits").
    * @return List of files as a list of String.
    *
    */
  def getListOfFiles(it: RemoteIterator[LocatedFileStatus],
                     extensions: List[String] = List(".fits")): List[String] = {
    if (!it.hasNext) {
      Nil
    } else {
      it.next.getPath.toString :: getListOfFiles(it, extensions)
    }
  }

  /**
    * Check that the schemas of different FITS HDU to be added are
    * the same. Throw an AssertionError otherwise.
    * The check is performed only for BINTABLE.
    *
    * NOTE: This operation is very long for many files! Do not use it for
    * hundreds of files!
    *
    * @param listOfFitsFiles : (List[String])
    *   List of files as a list of String.
    * @return (String) the type of HDU: BINTABLE, IMAGE, EMPTY, or
    *   NOT UNDERSTOOD if not registered.
    *
    */
  def checkSchemaAndReturnType(listOfFitsFiles : List[String], conf: Configuration): Boolean = {
    // Targeted HDU
    val indexHDU = conf.get("hdu").toInt

    // Initialise
    val path_init = new Path(listOfFitsFiles(0))

    val fits_init = new Fits(path_init, conf, indexHDU)

    if (fits_init.hdu.implemented) {
      // Do not perform checks if the mode is PERMISSIVE.
      if (conf.get("mode") != "PERMISSIVE") {
        val schema_init = getSchema(fits_init)
        fits_init.data.close()
        for (file <- listOfFitsFiles.slice(1, listOfFitsFiles.size)) {
          var path = new Path(file)
          val fits = new Fits(path, conf, indexHDU)
          val schema = getSchema(fits)
          val isOk = schema_init == schema
          isOk match {
            case true => isOk
            case false => {
              throw new AssertionError(
                """
              You are trying to add HDU data with different structures!
              Check that the number of columns, names of columns and element
              types are the same. re-run with .option("verbose", true) to
              list all the files.
            """)
            }
          }
          fits.data.close()
        }
      }
      true
    } else {
      println(s"""
        FITS type ${fits_init.hduType} not supported yet.
        An empty DataFrame will be returned.""")
      false
    }
  }
}
