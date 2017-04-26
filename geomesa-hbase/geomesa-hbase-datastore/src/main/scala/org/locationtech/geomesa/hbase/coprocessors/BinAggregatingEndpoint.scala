package org.locationtech.geomesa.hbase.coprocessors

import java.io._

import com.google.protobuf.{ByteString, RpcCallback, RpcController, Service}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import org.apache.hadoop.hbase.regionserver.InternalScanner
import org.apache.hadoop.hbase.{Cell, Coprocessor, CoprocessorEnvironment}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.formats.bin.BinAggregator
import org.locationtech.geomesa.formats.bin.BinOptions._
import org.locationtech.geomesa.hbase.coprocessors.BinAggregatingEndpoint._
import org.locationtech.geomesa.hbase.proto.BinAggregatingProto
import org.locationtech.geomesa.hbase.proto.BinAggregatingProto.{BinAggregatingRequest, BinAggregatingResponse}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

class BinAggregatingEndpoint extends BinAggregatingProto.BinAggregatingService with Coprocessor with  CoprocessorService with LazyLogging {
  private var env: RegionCoprocessorEnvironment = _

  override def getBinQuery(controller: RpcController, request: BinAggregatingRequest, done: RpcCallback[BinAggregatingRequest]): Unit = {
    var response : BinAggregatingResponse = null
    var scanner : InternalScanner = null
    val res: List[String] = null
    try {
      val scan = new Scan
      val options: Map[String, String] = deserializeOptions(request.getOptions.toByteArray)
      val sft = SimpleFeatureTypes.createType("input", options(RAW_SFT_OPT))
      val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

      scanner = env.getRegion.getScanner(scan)
      val results = new java.util.ArrayList[Cell]
      var hasMore = false
      do {
        hasMore = scanner.next(results)
        for (cell <- results) {
          val c = cell
          val sf = serializer.deserialize(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          aggregateResult(sf, res)
        }
        results.clear()
      } while (hasMore)

      // TODO encode result of crylo
      val result: Array[Byte] = null //KryoLazyDensityUtils.encodeResult(res)
      response = BinAggregatingResponse.newBuilder.setBinFeature(ByteString.copyFrom(result)).build
    } catch {
      case ioe: IOException =>
        ResponseConverter.setControllerException(controller, ioe)
      case t: Throwable =>
        // TODO why handle non io exceptions differently?
        logger.error("Error in BinAggregating Endpoint", t)
    } finally {
      CloseWithLogging(scanner)
    }
    done.run(request)
  }

  // todo not a stirng
  def aggregateResult(sf: SimpleFeature, res: List[String]): Unit = {

  }

  override def stop(env: CoprocessorEnvironment): Unit = {

  }

  override def start(env: CoprocessorEnvironment): Unit = {
    env match {
      case environment: RegionCoprocessorEnvironment => this.env = environment
      case _ => throw new CoprocessorException("Must be loaded on a table region!")
    }
  }

  override def getService: Service = this
}

object BinAggregatingEndpoint {

  @throws[IOException]
  def serializeOptions(map: Map[String, String]): Array[Byte] = {
    val fis = new ByteArrayOutputStream
    val ois = new ObjectOutputStream(fis)
    ois.writeObject(map)
    ois.flush()
    val output = fis.toByteArray
    ois.close()
    output
  }

  def deserializeOptions(bytes: Array[Byte]): Map[String, String] = {
    val byteIn = new ByteArrayInputStream(bytes)
    val in = new ObjectInputStream(byteIn)
    val map = in.readObject.asInstanceOf[Map[String, String]]
    map
  }

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", BinAggregator.BinSft)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    sf.values(0) = bytes
    sf
  }
}
