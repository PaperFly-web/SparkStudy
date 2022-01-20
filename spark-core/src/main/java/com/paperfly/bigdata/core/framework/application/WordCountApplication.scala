package com.paperfly.bigdata.core.framework.application

import com.paperfly.bigdata.core.framework.common.TApplication
import com.paperfly.bigdata.core.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  start(){
    val controller = new WordCountController()
    controller.dispatch()
  }

}
