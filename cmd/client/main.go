package main

import (
	"context"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

type UI struct {
	App         *tview.Application
	Grid        *tview.Grid
	Pages       *tview.Pages
	TopicList   *tview.List
	MessageList *tview.List
	MsgForm     *tview.Form
	MsgEditForm *tview.Form
	TopicForm   *tview.Form
}

func prepareUI(client_app *tview.Application) *UI {
	box_grid := tview.NewGrid().SetRows(-1).SetColumns(-1, -1, -1)

	pages := tview.NewPages()
	editable_part := tview.NewGrid().SetRows(5, 5).SetColumns(-1)

	message_form := tview.NewForm().
		AddInputField("Message", "", 50, nil, nil).
		AddButton("Send", func() {}).
		AddButton("Back", func() {
			pages.SwitchToPage("Select_action")
		})

	msg_edit_form := tview.NewForm().
		AddInputField("Message", "", 50, nil, nil).
		AddButton("Submit", func() {}).
		AddButton("Like", func() {}).
		AddButton("Back", func() {
			pages.SwitchToPage("Select_action")
		})

	topic_form := tview.NewForm().
		AddInputField("Topic name", "", 30, nil, nil).
		AddButton("Submit topic", func() {}).
		AddButton("Back", func() {
			pages.SwitchToPage("Select_action")
		})

	message_button := tview.NewButton("Send a message").
		SetSelectedFunc(func() {
			pages.SwitchToPage("New_message")
			client_app.SetFocus(message_form)
		})
	topic_button := tview.NewButton("Make a new topic").
		SetSelectedFunc(func() {
			pages.SwitchToPage("New_topic")
			client_app.SetFocus(topic_form)
		})
	editable_part.AddItem(message_button, 0, 0, 1, 1, 0, 0, true)
	editable_part.AddItem(topic_button, 1, 0, 1, 1, 0, 0, false)

	pages.AddPage("New_message", message_form, true, false)
	pages.AddPage("Edit_message", msg_edit_form, true, false)
	pages.AddPage("New_topic", topic_form, true, false)
	pages.AddPage("Select_action", editable_part, true, true)

	topic_list := tview.NewList()
	message_list := tview.NewList()

	box_grid.AddItem(topic_list, 0, 0, 1, 1, 0, 0, false)
	box_grid.AddItem(message_list, 0, 1, 1, 1, 0, 0, false)
	box_grid.AddItem(pages, 0, 2, 1, 1, 0, 0, true)

	ui := &UI{
		App:         client_app,
		Grid:        box_grid,
		Pages:       pages,
		TopicList:   topic_list,
		MessageList: message_list,
		MsgForm:     message_form,
		MsgEditForm: msg_edit_form,
		TopicForm:   topic_form,
	}

	return ui
}

func main() {
	// Connect to HEAD node (default)
	headConn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer headConn.Close()

	ctx := context.Background()

	// Get cluster state to find HEAD and TAIL addresses
	cp := pb.NewControlPlaneClient(headConn)
	state, err := cp.GetClusterState(ctx, &emptypb.Empty{})
	if err != nil {
		log.Printf("Warning: GetClusterState failed, using single node mode: %v", err)
		// Fallback to single node
		state = &pb.GetClusterStateResponse{
			Head: &pb.NodeInfo{Address: "127.0.0.1:50051"},
			Tail: &pb.NodeInfo{Address: "127.0.0.1:50051"},
		}
	}

	log.Printf("Connected to chain: HEAD=%s TAIL=%s", state.Head.Address, state.Tail.Address)

	// Connect to HEAD for writes
	if state.Head.Address != "127.0.0.1:50051" {
		headConn.Close()
		headConn, err = grpc.Dial(state.Head.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal("Connect to HEAD:", err)
		}
		defer headConn.Close()
	}
	mbWrite := pb.NewMessageBoardClient(headConn)

	// Connect to TAIL for reads
	tailConn, err := grpc.Dial(state.Tail.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("Connect to TAIL:", err)
	}
	defer tailConn.Close()
	mbRead := pb.NewMessageBoardClient(tailConn)

	client_app := tview.NewApplication()
	ui := prepareUI(client_app)

	// Helper functions that use both HEAD (write) and TAIL (read) clients
	var t *pb.Topic
	var reloadMessages func(*pb.Topic, *pb.User) // forward declaration
	
	reloadMessages = func(topic *pb.Topic, u *pb.User) {
		topic_messages, err := mbRead.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.Id})
		if err != nil {
			log.Printf("GetMessages error: %v", err)
			return
		}
		ui.MessageList.Clear()
		for _, current_msg := range topic_messages.Messages {
			msg := current_msg // capture for closure
			ui.MessageList.AddItem(msg.Text, "", 0, func() {
				if item := ui.MsgEditForm.GetFormItem(0); item != nil {
					item.(*tview.InputField).SetText(msg.Text)
				}
				if msg.UserId != u.Id {
					ui.MsgEditForm.GetButton(0).SetDisabled(true)
					ui.MsgEditForm.GetButton(0).SetBackgroundColor(tcell.Color(100))
					ui.MsgEditForm.GetButton(0).SetLabelColor(tcell.Color(0))
					ui.MsgEditForm.GetFormItem(0).SetDisabled(true)
				} else {
					ui.MsgEditForm.GetButton(0).SetDisabled(false)
					ui.MsgEditForm.GetFormItem(0).SetDisabled(false)
				}
				ui.MsgEditForm.GetButton(0).SetSelectedFunc(func() {
					if item := ui.MsgEditForm.GetFormItem(0); item != nil {
						text := item.(*tview.InputField).GetText()
						// UPDATE on HEAD (write)
						updatedMsg, _ := mbWrite.UpdateMessage(ctx, &pb.UpdateMessageRequest{
							TopicId: topic.Id, UserId: u.Id, MessageId: msg.Id, Text: text})
						if updatedMsg != nil {
							reloadMessages(topic, u)
						}
						ui.Pages.SwitchToPage("Select_action")
					}
				})
				ui.MsgEditForm.GetButton(1).SetSelectedFunc(func() {
					// LIKE on HEAD (write)
					mbWrite.LikeMessage(ctx, &pb.LikeMessageRequest{
						TopicId: topic.Id, UserId: u.Id, MessageId: msg.Id})
					reloadMessages(topic, u)
					ui.Pages.SwitchToPage("Select_action")
				})
				ui.Pages.SwitchToPage("Edit_message")
			})
		}
	}

	// shared setup - CREATE USER on HEAD (write)
	u, err := mbWrite.CreateUser(ctx, &pb.CreateUserRequest{Name: "ana"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}

	// GET TOPICS from TAIL (read)
	get_all_topics, err := mbRead.ListTopics(ctx, &emptypb.Empty{})
	if err != nil {
		log.Fatal("ListTopics:", err)
	}

	for topic_id := range get_all_topics.Topics {
		current_topic := get_all_topics.Topics[topic_id]
		ui.TopicList.AddItem(current_topic.Name, "", 0, func() {
			t = current_topic
			reloadMessages(t, u)
		})
	}

	ui.TopicForm.GetButton(0).SetSelectedFunc(func() {
		if item := ui.TopicForm.GetFormItem(0); item != nil {
			text := item.(*tview.InputField).GetText()
			// CREATE TOPIC on HEAD (write)
			top, err := mbWrite.CreateTopic(ctx, &pb.CreateTopicRequest{Name: text})
			if err != nil {
				log.Fatal("CreateTopic:", err)
			}
			item.(*tview.InputField).SetText("")
			ui.TopicList.AddItem(top.Name, "", 0, func() {
				t = top
				reloadMessages(t, u)
			}).SetCurrentItem(int(top.Id))
			t = top
			reloadMessages(t, u)
			ui.Pages.SwitchToPage("Select_action")
		}
	})

	ui.MsgForm.GetButton(0).SetSelectedFunc(func() {
		if item := ui.MsgForm.GetFormItem(0); item != nil {
			text := item.(*tview.InputField).GetText()
			if t != nil {
				// POST MESSAGE on HEAD (write)
				_, err := mbWrite.PostMessage(ctx, &pb.PostMessageRequest{TopicId: t.Id, UserId: u.Id, Text: text})
				if err != nil {
					log.Fatal("PostMessage:", err)
				}
				reloadMessages(t, u)
			}
			item.(*tview.InputField).SetText("")
			ui.Pages.SwitchToPage("Select_action")
		}
	})

	/*
		go func() {
			for {
				topic_messages, err := mb.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: t.Id})
				if err != nil {
					log.Fatal("GetMessages:", err)
				}

				if len(current_messages) > 0 {
					for msg_id := range topic_messages.Messages {
						current_msg := topic_messages.Messages[msg_id]
						if current_msg != current_messages[msg_id] {
							ui.App.QueueUpdateDraw(func() {
								ui.MessageList.RemoveItem(msg_id)
								ui.MessageList.InsertItem(msg_id, current_msg.Text, "", 0, func() {})
								current_messages[msg_id] = current_msg
							})
						}
					}
				}
				if len(current_messages) > 0 && len(topic_messages.Messages) > 0 {
					for msg_id := range topic_messages.Messages {
						current_msg := topic_messages.Messages[msg_id]
						ui.App.QueueUpdateDraw(func() {
							ui.MessageList.RemoveItem(msg_id)
							ui.MessageList.InsertItem(msg_id, current_msg.Text, "", 0, func() {})
							current_messages = append(current_messages, current_msg)
						})
					}
				}
			}
		}()
	*/

	/*
		t, err := mb.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "prva tema"})
		if err != nil {
			log.Fatal("CreateTopic:", err)
		}

		if mode == "sub" {
			state, err := cp.GetClusterState(ctx, &emptypb.Empty{})
			if err != nil {
				log.Fatal("GetClusterState:", err)
			}
			fmt.Println("Cluster head/tail:", state.Head.Address, state.Tail.Address)

			subNode, err := mb.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
				UserId:  u.Id,
				TopicId: []int64{t.Id},
			})
			if err != nil {
				log.Fatal("GetSubscriptionNode:", err)
			}
			fmt.Println("SubNode:", subNode.Node.Address, "token=", subNode.SubscribeToken)

			stream, err := mb.SubscribeTopic(context.Background(), &pb.SubscribeTopicRequest{
				TopicId:        []int64{t.Id},
				UserId:         u.Id,
				FromMessageId:  0,
				SubscribeToken: subNode.SubscribeToken,
			})
			if err != nil {
				log.Fatal("SubscribeTopic:", err)
			}
			fmt.Println("Subscribed. Waiting for events...")

			for {
				ev, err := stream.Recv()
				if err != nil {
					log.Fatal("stream recv:", err)
				}
				fmt.Printf("EVENT seq=%d op=%s topic=%d msg=%d text=%q likes=%d\n",
					ev.SequenceNumber, ev.Op.String(),
					ev.Message.TopicId, ev.Message.Id, ev.Message.Text, ev.Message.Likes)
			}
		}

		// demo mode: povzroči nekaj eventov
		posted, err := mb.PostMessage(ctx, &pb.PostMessageRequest{
			TopicId: t.Id,
			UserId:  u.Id,
			Text:    "hello world",
		})
		if err != nil {
			log.Fatal("PostMessage:", err)
		}

		_, err = mb.LikeMessage(ctx, &pb.LikeMessageRequest{
			TopicId:   t.Id,
			MessageId: posted.Id,
			UserId:    u.Id,
		})
		if err != nil {
			log.Fatal("LikeMessage:", err)
		}

		_, err = mb.UpdateMessage(ctx, &pb.UpdateMessageRequest{
			TopicId:   t.Id,
			UserId:    u.Id,
			MessageId: posted.Id,
			Text:      "edited text",
		})
		if err != nil {
			log.Fatal("UpdateMessage:", err)
		}

		_, err = mb.DeleteMessage(ctx, &pb.DeleteMessageRequest{
			TopicId:   t.Id,
			UserId:    u.Id,
			MessageId: posted.Id,
		})
		if err != nil {
			log.Fatal("DeleteMessage:", err)
		}

		fmt.Println("demo done")
	*/

	err = client_app.SetRoot(ui.Grid, true).EnableMouse(true).Run()
	if err != nil {
		log.Fatal("Create box:", err)
	}
}
