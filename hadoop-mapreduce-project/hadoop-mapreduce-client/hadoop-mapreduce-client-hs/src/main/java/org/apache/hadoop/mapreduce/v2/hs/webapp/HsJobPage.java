/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import static org.apache.hadoop.mapreduce.v2.app.webapp.AMParams.JOB_ID;
import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.ACCORDION;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI.initID;

import org.apache.hadoop.yarn.webapp.SubView;

/**
 * Render a page that describes a specific job.
 */
public class HsJobPage extends HsView {

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.hs.webapp.HsView#preHead(org.apache.hadoop.yarn.webapp.hamlet.Hamlet.HTML)
   */
  @Override protected void preHead(Page.HTML<__> html) {
    String jobID = $(JOB_ID);
    set(TITLE, jobID.isEmpty() ? "Bad request: missing job ID"
               : join("MapReduce Job ", $(JOB_ID)));
    commonPreHead(html);
    //Override the nav config from the commonPreHead
    set(initID(ACCORDION, "nav"), "{autoHeight:false, active:1}");
  }

  /**
   * The content of this page is the JobBlock
   * @return HsJobBlock.class
   */
  @Override protected Class<? extends SubView> content() {
    return HsJobBlock.class;
  }
}
