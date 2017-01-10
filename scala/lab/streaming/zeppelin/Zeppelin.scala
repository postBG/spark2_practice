package lab.streaming.zeppelin

/**
 * 
 1. Zeppelin (0.6.1) 다운로드 on CentOS7....
  - # cd /kikang
  - # wget http://mirror.apache-kr.org/zeppelin/zeppelin-0.6.1/zeppelin-0.6.1-bin-all.tgz
  - # tar xvfz zeppelin-0.6.1-bin-all.tgz
 
 2. Zeppelin 설정....(포트변경.... 8080 => 9090)
  - # cd /kikang/zeppelin-0.6.1-bin-all/conf
  - # cp zeppelin-site.xml.template zeppelin-site.xml
  - # vi zeppelin-site.xml
        <property>
          <name>zeppelin.server.port</name>
          <value>9090</value>	//-- 8080을 9090으로 변경.... Spark Master Web UI Port 8080과 충돌 방지....
          <description>Server port.</description>
        </property>
        
 3. Zeppelin 시작....
   - # cd /kikang/zeppelin-0.6.1-bin-all/bin
   - # ./zeppelin-daemon.sh start			//--ZeppleinServer 시작....
   - # jps 	//--ZeppelinServer 프로세스 확인....
   - Zepplin 접속 (http://CentOS7-14:9090)
   
 4. Interpreter 설정....
   - Zepplin 접속 (http://CentOS7-14:9090)
   - 우측 상단 Interpreter 메뉴 클릭
   - spark 인터프리터 영역 우측 edit 버튼 클릭
   - 아래 Dependencies 항목의 artifact에 "org.apache.bahir:spark-streaming-twitter_2.11:2.0.0" 입력 후 Save 버튼 클릭
   
 5. Twitter 스트리밍 코드 + SQL 작성 및 실행....
   

 * 
 */
object Zeppelin {
  
}