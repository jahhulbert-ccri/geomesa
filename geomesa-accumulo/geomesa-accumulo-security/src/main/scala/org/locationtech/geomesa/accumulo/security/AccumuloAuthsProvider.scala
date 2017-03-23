package org.locationtech.geomesa.accumulo.security

import java.nio.charset.StandardCharsets

import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.security.AuthorizationsProvider

import scala.collection.JavaConversions._

class AccumuloAuthsProvider(val authProvider: AuthorizationsProvider) {

  def getAuthorizations: Authorizations = {
    new Authorizations(authProvider.getAuthorizations.map(_.getBytes(StandardCharsets.UTF_8)))
  }
}
