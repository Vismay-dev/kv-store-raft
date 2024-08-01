package kvservice

type Clerk struct {
	servers  []*Server
	clientId int64
	leaderId int64
}
