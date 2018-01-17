package com.yurun.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree;

/**
 * Created by yurun on 18/1/17.
 */
public class ProcessTreeMain {

  public static void main(String[] args) {
    ProcfsBasedProcessTree processTree = new ProcfsBasedProcessTree(args[0], "/proc");

    processTree.setConf(new Configuration());

    processTree.updateProcessTree();

    System.out.println(processTree.getCurrentProcessIDs());
    System.out.println(processTree.getProcessTreeDump());

    System.out.println(processTree.getCumulativeVmem());
    System.out.println(processTree.getCumulativeRssmem());

    System.out.println(ProcfsBasedProcessTree.PAGE_SIZE);
  }

}
