package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var myAddress string = "Q1"
var peerAddress []string
var term = 0
var termFileContents string = ""
var myLogArray []string
func initializeLog() {
	myLogArray = append(myLogArray, "0.0.0")
	myLogArray = append(myLogArray, "1.0.0")
	myLogArray = append(myLogArray, "2.0.0")
	initializeTermFile()
}

func initializeTermFile() {
		term = 0
}

func getLastLines(lines int) string{
	stringArray := myLogArray[len(myLogArray)-lines:len(myLogArray)]
	justString := strings.Join(stringArray,",")
	return justString
}

func getCompleteLogArray() []string{
	return myLogArray
}


func writeToLogFile(s string, file *os.File){
	last := getLastLineWithSeek(file)
	s_index,err := strconv.Atoi(strings.Split(last,".")[0])
	s_index++
	_,err = file.WriteString(strconv.Itoa(s_index)+"."+strconv.Itoa(term)+"."+s)
	check(err)
}
func incrementTerm() {
	//fmt.Println("call to increment term")
	term++
}


func updateTerm(n int) {
	fmt.Println("call to update term")
	term = n
}

func declareQueue(ch *amqp.Channel) error{
	_, err := ch.QueueDeclare(
		myAddress, // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		true,   // no-wait
		nil,     // arguments
	)
	return err
}

func declareConsumeMessage(ch *amqp.Channel) (<-chan amqp.Delivery,error){
	msgs, err := ch.Consume(
		myAddress, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		true,  // no-wait
		nil,    // args
	)
	return msgs,err
}

func getLastLineWithSeek(filepath *os.File) string {
	fileHandle := filepath

	line := ""
	var cursor int64 = 0
	stat, _ := fileHandle.Stat()
	filesize := stat.Size()
	for {
		cursor -= 1
		fileHandle.Seek(cursor, io.SeekEnd)

		char := make([]byte, 1)
		fileHandle.Read(char)

		if cursor != -1 && (char[0] == 10 || char[0] == 13) { // stop if we find a line
			break
		}

		line = fmt.Sprintf("%s%s", string(char), line) // there is more efficient way

		if cursor == -filesize { // stop if we are at the beginning
			break
		}
	}

	return line
}
var globalCh *amqp.Channel
var leaderCommit = 2
var repliesMap map[string]int
func main() {

	argsWithProg := os.Args
	arg := argsWithProg[1]
	if arg == "1"{
		myAddress="Q1"
	}else if arg =="2"{
		myAddress="Q2"
	}else if arg=="3"{
		myAddress="Q3"
	}else{
		myAddress="Q1"
	}
	castVotesPerTerm = make(map[int]string)
	recievedVotesPerTerm = make(map[int][]string)
	repliesMap = make(map[string]int)
	fmt.Println("myAddress",myAddress)
	initializeLog()
	peerAddress = []string{"Q1","Q2","Q3"}
	conn, err := amqp.Dial("amqp://test:test@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = declareQueue(ch)
	failOnError(err, "Failed to declare a queue")

	msgs, err := declareConsumeMessage(ch)
	failOnError(err, "Failed to register a consumer")
	go input(func(line string) {
		chat(line,ch)
	})
	//forever := make(chan bool)
	go handleMSG(msgs)
	go runContinuously(ch)
	globalCh = ch
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	// Close stdin to kill the input goroutine.
	check(os.Stdin.Close())

	// Empty println.
	println()
}
var sendMessageTime time.Time
var copiedTime time.Time
func chat(line string, ch *amqp.Channel) {
	switch line {
	case "/discover":
		//discover(overlay)
		return
	case "/peers":
		//peers(overlay)
		return
	case "/tr":
		printTransactionArray()
		return
	case "/te":
		fmt.Println("myterm",term)
		return
	default:
	}


	if strings.HasPrefix(line, "/") {
		//help(node)
		return
	}
	if getCurrentRole()!="leader"{
		fmt.Println("you can't push to the system")
		return
	}
	sendMessageTime = time.Now()
	for k := range repliesMap {
		delete(repliesMap, k)
	}
	if strings.Contains(line,"+") || strings.Contains(line,"-"){
		//broadcastMessageToAllPeers(line,ch)
		n:=getNumberOfLines()
		//check(err)
		addToMyLog(line,n)
	}
}

func printTransactionArray() {
	fmt.Println("myLogArray",myLogArray)
}
func getNumberOfLines() (int) {
	return len(myLogArray)

}

func lineCounter(r io.Reader) (int) {
	p,err := ioutil.ReadAll(r)
	check(err)
	s := string(p)
	st := strings.Split(s,"\n")
	return len(st)
}

func addToMyLog(line string,n int) {
	myLog := strconv.Itoa(n)+"."+strconv.Itoa(term)+"."+line
	myLogArray = append(myLogArray,myLog)
}

// check panics if err is not nil.
func check(err error) {
	if err != nil {
		panic(err)
	}
}
// input handles inputs from stdin.
func input(callback func(string)) {
	r := bufio.NewReader(os.Stdin)

	for {
		buf, _, err := r.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			check(err)
		}

		line := string(buf)
		if len(line) == 0 {
			continue
		}

		callback(line)
	}
}
func handleMSG(msgs <-chan amqp.Delivery)  {
	//log.Println(len(msgs))
	for d := range msgs {
		log.Printf("Received a message: %s", d.Body)
		takeActionOnMessage(d.Body)
	}
}
var recievedVotesPerTerm map[int][]string

func takeActionOnMessage(body []byte) {
	//fmt.Println("TAKE ACTION ON MESSAGE")
	s := string(body)
	fmt.Println(s)
	if strings.HasPrefix(s,"requestVote"){
		fmt.Println("Someone asked for vote")
		parseRequestVoteAndHandle(s)
	}else if strings.HasPrefix(s,"positiveVote"){
		otherAddress := strings.Replace(s,"positiveVote_","",-1)
		//check(err)
		listOfVotes := recievedVotesPerTerm[term]
		for _, s := range listOfVotes {
			if otherAddress==s{
				return
			}
		}
		//fmt.Println("aaaaaaaaaaaaaaa")
		recievedVotesPerTerm[term] = append(recievedVotesPerTerm[term], otherAddress)
		//fmt.Println("bbbbbbbbbbbbbbb")
		//log.Println("votecount",len(recievedVotesPerTerm[term]))
		if len(recievedVotesPerTerm[term]) >= len(peerAddress)/2{
			if len(recievedVotesPerTerm[term]) ==len(peerAddress)/2{
				//got enough votes time to step up as leader
				setCurrentRole("leader")
				//fmt.Println("I will lead the world")
				bacameLeader = time.Now()
				diff := bacameLeader.Sub(becameCandidate)
				fmt.Println("time taken in election",diff.Milliseconds())
				broadcastMessageToAllPeers("appendEntries"+"_"+strconv.Itoa(term)+"_"+myAddress+"_"+getLastLines(2)+"_"+strconv.Itoa(leaderCommit),globalCh)
			}else{
				//do nothing
			}
		}
	}else if strings.HasPrefix(s,"negativeVote"){
		//fmt.Println("became follower 1")
		setCurrentRole("follower")
		lastTimeOut = time.Now().Add(4*time.Second)
	}else if strings.HasPrefix(s,"appendEntries"){
		//fmt.Println("became follower 2")
		setCurrentRole("follower")
		lastTimeOut = time.Now().Add(8*time.Second)
		appendEntriesArray := strings.Split(s,"_")
		leadersTermString := appendEntriesArray[1]
		leadersTerm ,err:= strconv.Atoi(leadersTermString)
		check(err)
		leaderAddressString := appendEntriesArray[2]
		leaderEntries := appendEntriesArray[3]
		//leaderCommit,err := strconv.Atoi(appendEntriesArray[4])
		leaderEntriesArray := strings.Split(leaderEntries,",")
		//fmt.Println("leaderEntriesArray",leaderEntriesArray)
		lengthLeaderEntriesArray := len(leaderEntriesArray)

		//fmt.Println("lengthLeaderEntriesArray",lengthLeaderEntriesArray)
		if (leadersTerm < term){
			//fmt.Println("case 1")
			// i am having greater term i should become leader but i have to obey now
			msg := "reply_"+strconv.Itoa(term) +"_" +strconv.Itoa(lengthLeaderEntriesArray)+"_"+myAddress+"_"+"0"
			sendMessageToPeer(msg,leaderAddressString)
		}else{
			startPoint:=leaderEntriesArray[0]
			endPoint := leaderEntriesArray[len(leaderEntriesArray)-1]
			startPointIndex,err := strconv.Atoi(strings.Split(startPoint,".")[0])
			check(err)
			endPointIndex,err :=strconv.Atoi( strings.Split(endPoint,".")[0])
			myLogArrayTemp := getCompleteLogArray()

			if ((len(myLogArrayTemp)-1) < startPointIndex){
				//fmt.Println("case 2")
				msg := "reply_"+strconv.Itoa(term) +"_" +strconv.Itoa(lengthLeaderEntriesArray)+"_"+myAddress+"_"+"0"
				sendMessageToPeer(msg,leaderAddressString)
				updateTerm(leadersTerm)
			}else{
				if term < leadersTerm{
					updateTerm(leadersTerm)
				}
				//fmt.Println("case 3")
				matches := false
				for i:=startPointIndex;i<=endPointIndex;i++{
					if (myLogArrayTemp[startPointIndex] == leaderEntriesArray[i-startPointIndex]){
						matches=true
						break
					}
				}
				//fmt.Println("matches",matches)
				if matches==true{
					//fmt.Println("case 5")
					// replace log and write new log
					//fmt.Println("startPointIndex",startPointIndex)
					//fmt.Println("endPointIndex",endPointIndex)
					//fmt.Println("myLogArrayTemp",myLogArrayTemp)
					initialLengthOfMyLogArray := len(myLogArrayTemp)
					for i:=startPointIndex;i<=endPointIndex ;i++{
						//fmt.Println("inside for",initialLengthOfMyLogArray-1,i)
						if initialLengthOfMyLogArray -1 < i{
							//fmt.Println("myLogArray2",leaderEntriesArray[i-startPointIndex])
							myLogArrayTemp = append(myLogArrayTemp,leaderEntriesArray[i-startPointIndex])
						}else{
							//fmt.Println("myLogArray1",myLogArrayTemp[i],leaderEntriesArray[i-startPointIndex])
							myLogArrayTemp[i] = leaderEntriesArray[i-startPointIndex]
						}
					}
					myLogArrayTemp = myLogArrayTemp[0:endPointIndex+1]
					//fmt.Println("After myLogArrayTemp",myLogArrayTemp)
					// write back to log file
					myLogArray = myLogArrayTemp
					msg := "reply_"+strconv.Itoa(term) +"_" +strconv.Itoa(lengthLeaderEntriesArray)+"_"+myAddress+"_"+"1"
					sendMessageToPeer(msg,leaderAddressString)
				}else{
					//fmt.Println("case 4")
					// ask for more log entries
					msg := "reply_"+strconv.Itoa(term) +"_" +strconv.Itoa(lengthLeaderEntriesArray)+"_"+myAddress+"_"+"0"
					sendMessageToPeer(msg,leaderAddressString)
				}
			}
		}
	}else if strings.HasPrefix(s,"reply_"){
		if strings.HasSuffix(s,"1"){
			// do nothing
			// log is up to date
			fmt.Println("got one in reply")
			if _, ok := repliesMap[s]; !ok {
				fmt.Println("inside replies map")
				repliesMap[s]=1
				copiedTime = time.Now()
				diff := copiedTime.Sub(sendMessageTime)
				fmt.Println("Message Copying Time",diff.Milliseconds())
			}
		}else{
			// parse reply
			//fmt.Println("got zero in reply")
			v,err:= strconv.Atoi(strings.Split(s,"_")[2])
			//fmt.Println(v)
			nodeAddress := strings.Split(s,"_")[3]
			//fmt.Println(nodeAddress)
			check(err)
			sendMessageToPeer("appendEntries"+"_"+strconv.Itoa(term)+"_"+myAddress+"_"+getLastLines(v+1)+"_"+strconv.Itoa(leaderCommit),nodeAddress)
		}
	}
}

var castVotesPerTerm map[int]string
func parseRequestVoteAndHandle(s string) {
	ss := strings.Split(s,"_")
	otherTermString :=  ss[1]
	otherCandidateName := ss[2]
	otherCandidateLastTransaction := ss[3]
	otherCandidateLastTransactionArray := strings.Split(otherCandidateLastTransaction,".")
	otherCandidateLastTransactionIndex := otherCandidateLastTransactionArray[0]
	otherCandidateLastTransactionTerm := otherCandidateLastTransactionArray[1]
	myLastLine := getLastLines(1)
	myLastLineArray := strings.Split(myLastLine,".")
	myLastTransactionIndex := myLastLineArray[0]
	myLastTransactionTerm := myLastLineArray[1]
	otherTerm,err := strconv.Atoi(otherTermString)
	check(err)

	if getCurrentRole()=="leader"{
		// do nothing

	}
	if (otherTerm < term){
		//fmt.Println(1111111111111111)
		sendMessageToPeer("negativeVote",otherCandidateName)
	}else{
		//fmt.Println(2222222222222222)
		print(castVotesPerTerm)
		if whom, ok := castVotesPerTerm[otherTerm]; ok {
			//the terms vote is casted already
			if whom == otherCandidateName{
				if myLastTransactionIndex <= otherCandidateLastTransactionIndex && myLastTransactionTerm <= otherCandidateLastTransactionTerm{
					//fmt.Println(333333333333333)
					castVotesPerTerm[otherTerm] = otherCandidateName
					//fmt.Println("myterm",term)
					sendMessageToPeer("positiveVote_"+myAddress,otherCandidateName)
					//fmt.Println("became follower 3")
					setCurrentRole("follower")
					lastTimeOut = time.Now().Add(4*time.Second)
				}else{
					//fmt.Println(4444444444444444)
					sendMessageToPeer("negativeVote",otherCandidateName)
				}
			}else{
				sendMessageToPeer("negativeVote",otherCandidateName)
			}
		}else{
			// the terms vote is not casted yet
			if myLastTransactionIndex <= otherCandidateLastTransactionIndex && myLastTransactionTerm <= otherCandidateLastTransactionTerm{
				//fmt.Println(333333333333333)
				castVotesPerTerm[otherTerm] = otherCandidateName
				//fmt.Println("myterm",term)
				sendMessageToPeer("positiveVote_"+myAddress,otherCandidateName)
			}else{
				//fmt.Println(4444444444444444)
				sendMessageToPeer("negativeVote",otherCandidateName)
			}
		}
	}
}


func publish(address string,msg string) error{
	err := globalCh.Publish(
		"",     // exchange
		address, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
			Expiration: "100",
		})
	return err
}
var currentRole string = "follower"
var lastTimeOut time.Time = time.Now()
var m sync.Mutex
var currentTerm = 1
func setCurrentRole(role string)  {
	m.Lock()
	currentRole = role
	m.Unlock()
}

func getCurrentRole() string {
	return currentRole
}

func LineCounter(r io.Reader) (int, error) {

	var count int
	const lineBreak = '\n'

	buf := make([]byte, bufio.MaxScanTokenSize)

	for {
		bufferSize, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}

		var buffPosition int
		for {
			i := bytes.IndexByte(buf[buffPosition:], lineBreak)
			if i == -1 || bufferSize == buffPosition {
				break
			}
			buffPosition += i + 1
			count++
		}
		if err == io.EOF {
			break
		}
	}

	return count, nil
}
var becameCandidate time.Time
var bacameLeader time.Time

var linesSent map[string]int
func runContinuously(ch *amqp.Channel)  {
	for {
		if (getCurrentRole()=="follower"){
			n := getRandomNumber()
			time.Sleep(time.Duration(n) * time.Millisecond)
			//log.Println(lastTimeOut,time.Now())
			if (time.Now().After(lastTimeOut)){
				// time to step up as next possible leader candidate
				fmt.Println("became candidate")
				setCurrentRole("candidate")

			}
		}else if (getCurrentRole()=="candidate"){
			//waiting for election results
			//fmt.Println("waiting for election results")
			fmt.Println("became candidate")
			becameCandidate = time.Now()
			incrementTerm()
			initializeVotingParams()
			broadcastMessageToAllPeers("requestVote_"+strconv.Itoa(term)+"_"+myAddress+"_"+getLastLines(1),ch)
			time.Sleep(time.Duration(2000) * time.Millisecond)
		} else if (getCurrentRole()=="leader"){
			n := 1000
			time.Sleep(time.Duration(n) * time.Millisecond)

			//fmt.Println("doing leaders job")
			broadcastMessageToAllPeers("appendEntries"+"_"+strconv.Itoa(term)+"_"+myAddress+"_"+getLastLines(2)+"_"+strconv.Itoa(leaderCommit),ch)
		}
	}
}
var votes map[string]bool
func initializeVotingParams() {
	votes = nil
}

func getRandomNumber() int {
	min := 2000
	max := 3000
	return (rand.Intn(max - min) + min)
}

func broadcastMessageToAllPeers(msg string,ch *amqp.Channel) {
	for i:=0;i< len(peerAddress);i++{
		if peerAddress[i]==myAddress {
			continue
		}
		body := msg
		err := publish(peerAddress[i],body)
		failOnError(err, "Failed to publish a message")
		//log.Printf(" [x] Sent %s", body)
	}
}

func sendMessageToPeer(msg string,peer string) {
		body := msg
		err := publish(peer,body)
		failOnError(err, "Failed to publish a message")
		//log.Printf(" [x] Sent %s", body)
}
