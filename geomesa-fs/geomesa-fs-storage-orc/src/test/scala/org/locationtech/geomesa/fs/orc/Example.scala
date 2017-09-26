package org.locationtech.geomesa.fs.orc

import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, LongColumnVector, MapColumnVector}
import org.apache.orc.{OrcFile, TypeDescription}

object Example {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val schema = TypeDescription.fromString("struct<first:int,second:int,third:map<string,int>>")

    {

      val file = new Path("/tmp/geomesa.orc")
      val fs = file.getFileSystem(conf)
      if (fs.exists(file)) {
        fs.delete(file, true)
      }
      val writer = OrcFile.createWriter(file, OrcFile.writerOptions(conf).setSchema(schema))

      val batch = schema.createRowBatch()

      val first = batch.cols(0).asInstanceOf[LongColumnVector]
      val second = batch.cols(1).asInstanceOf[LongColumnVector]

      //Define map. You need also to cast the key and value vectors
      val map = batch.cols(2).asInstanceOf[MapColumnVector]
      val mapKey = map.keys.asInstanceOf[BytesColumnVector]
      val mapValue = map.values.asInstanceOf[LongColumnVector]

      // Each map has 5 elements
      val MAP_SIZE = 5
      val BATCH_SIZE = batch.getMaxSize

      // Ensure the map is big enough
      mapKey.ensureSize(BATCH_SIZE * MAP_SIZE, false)
      mapValue.ensureSize(BATCH_SIZE * MAP_SIZE, false)

      // add 1500 rows to file
      (0 until 15000).foreach { r =>
        val row = batch.size
        batch.size += 1

        first.vector(row) = r
        second.vector(row) = r * 3

        map.offsets(row) = map.childCount
        map.lengths(row) = MAP_SIZE
        map.childCount += MAP_SIZE


        var mapElem = map.offsets(row).toInt
        while (mapElem < map.offsets(row) + MAP_SIZE) {
          val key = "row " + r + "." + (mapElem - map.offsets(row))
          mapKey.setVal(mapElem, key.getBytes(StandardCharsets.UTF_8))
          mapValue.vector(mapElem) = mapElem
          mapElem += 1
        }

        if (row == BATCH_SIZE - 1) {
          writer.addRowBatch(batch)
          batch.reset()
        }
      }

      if (batch.size != 0) {
        writer.addRowBatch(batch)
        batch.reset()
      }
      writer.close()

    }

    {
      val reader = OrcFile.createReader(new Path("/tmp/geomesa.orc"), OrcFile.readerOptions(conf))

      val rows = reader.rows()
      val batch = reader.getSchema.createRowBatch()

      while (rows.nextBatch(batch)) {
        (0 until batch.size).foreach { r =>
          batch.cols.foreach { c => c }
        }
      }
      rows.close()
    }

  }

}
