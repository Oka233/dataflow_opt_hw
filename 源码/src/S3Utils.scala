import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import com.bingocloud.services.s3.model.ObjectMetadata
import org.nlpcn.commons.lang.util.IOUtil
import org.apache.commons.io.IOUtils

class S3Utils(bucket:String, accessKey:String, secretKey: String, endpoint: String) {
  val bucketName=bucket
  val credentials = new BasicAWSCredentials(accessKey, secretKey)
  val clientConfig = new ClientConfiguration()
  val amazonS3 = new AmazonS3Client(credentials, clientConfig)
  clientConfig.setProtocol(Protocol.HTTP)
  amazonS3.setEndpoint(endpoint)

  def read(bucket: String, key: String): String = {
    val s3Object = amazonS3.getObject(bucket, key)
    IOUtil.getContent(s3Object.getObjectContent, "UTF-8")
  }

  def write(txtMap: scala.collection.mutable.Map[String, String]): Unit = {
    var prefix = "res/"
    txtMap.keys.foreach { i => {
      val metadata = new ObjectMetadata
      metadata.setContentType("plain/text")
      metadata.addUserMetadata("title", "someTitle")
      amazonS3.putObject(bucketName, prefix + i, IOUtils.toInputStream(txtMap(i)), metadata)
    }
    }
  }
}