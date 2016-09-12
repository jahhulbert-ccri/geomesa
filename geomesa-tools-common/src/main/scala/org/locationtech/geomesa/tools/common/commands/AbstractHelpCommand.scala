/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.common.commands

import java.util

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.tools.common.commands.AbstractHelpCommand.HelpParameters

abstract class AbstractHelpCommand(parent: JCommander) extends Command(parent) {
  override val command = "help"
  override val params = new HelpParameters
}

object AbstractHelpCommand {
  @Parameters(commandDescription = "Show help")
  class HelpParameters {
    @Parameter(description = "commandName", required = false)
    val commandName: util.List[String] = new util.ArrayList[String]()
  }
}
