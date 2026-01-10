package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	pb "github.com/zanostro/razpravljalnica/gen/pb"
)

type UI struct {
	App                *tview.Application
	Grid               *tview.Grid
	Pages              *tview.Pages
	TopicList          *tview.List
	SubscribedList     *tview.List
	MessageList        *tview.List
	PopupMessage       *tview.TextView
	LikedList          *tview.List
	MsgForm            *tview.Form
	MsgEditForm        *tview.Form
	TopicForm          *tview.Form
	SubscriptionEvents *tview.TextView
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
	subscribed_list := tview.NewList()
	message_list := tview.NewList()
	liked_list := tview.NewList().SetSelectedFocusOnly(true)
	sub_message := tview.NewTextView()

	subscription_events := tview.NewTextView()
	subscription_events.SetBorder(true).SetTitle(" Subscribed Topics Messages ")
	subscription_events.SetDynamicColors(true)
	subscription_events.SetScrollable(true)

	box_grid.AddItem(subscribed_list, 0, 0, 3, 1, 0, 0, false)
	box_grid.AddItem(topic_list, 0, 1, 3, 3, 0, 0, false)
	box_grid.AddItem(message_list, 0, 4, 3, 3, 0, 0, false)
	box_grid.AddItem(sub_message, 3, 4, 1, 3, 0, 0, false)
	box_grid.AddItem(liked_list, 0, 7, 3, 1, 0, 0, false)
	box_grid.AddItem(pages, 0, 8, 2, 4, 0, 0, true)
	box_grid.AddItem(subscription_events, 2, 8, 2, 4, 0, 0, false)

	ui := &UI{
		App:                client_app,
		Grid:               box_grid,
		Pages:              pages,
		TopicList:          topic_list,
		SubscribedList:     subscribed_list,
		MessageList:        message_list,
		PopupMessage:       sub_message,
		LikedList:          liked_list,
		MsgForm:            message_form,
		MsgEditForm:        msg_edit_form,
		TopicForm:          topic_form,
		SubscriptionEvents: subscription_events,
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
	var reloadMessages func(*pb.Topic, *pb.User)
	var prev_topic *pb.Topic

	reloadMessages = func(topic *pb.Topic, u *pb.User) {
		topic_messages, err := mbRead.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: topic.Id})
		if err != nil {
			log.Printf("GetMessages error: %v", err)
			return
		}

		if prev_topic == nil || prev_topic.Id != topic.Id {
			ui.MessageList.Clear()
			ui.LikedList.Clear()
			prev_topic = topic
		}
		var msg_count int = 0
		for _, current_msg := range topic_messages.Messages {
			msg := current_msg

			if ui.LikedList.GetItemCount() <= msg_count {
				ui.LikedList.InsertItem(msg_count, strconv.Itoa(int(msg.Likes)), "", 0, func() {})
			} else {
				ui.LikedList.SetItemText(msg_count, strconv.Itoa(int(msg.Likes)), "")
			}

			if ui.MessageList.GetItemCount() <= msg_count {
				ui.MessageList.InsertItem(msg_count, msg.Text, "", 0, func() {
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
						mbWrite.LikeMessage(ctx, &pb.LikeMessageRequest{
							TopicId: topic.Id, UserId: u.Id, MessageId: msg.Id})
						reloadMessages(topic, u)
						ui.Pages.SwitchToPage("Select_action")
					})
					ui.Pages.SwitchToPage("Edit_message")
				})
			} else {
				ui.MessageList.SetItemText(msg_count, msg.Text, "")
			}

			msg_count += 1
		}
	}

	u, err := mbWrite.CreateUser(ctx, &pb.CreateUserRequest{Name: "ana"})
	if err != nil {
		log.Fatal("CreateUser:", err)
	}

	var subscriptions []string
	var reloadTopics func()

	reloadTopics = func() {
		get_all_topics, err := mbRead.ListTopics(ctx, &emptypb.Empty{})
		if err != nil {
			log.Fatal("ListTopics:", err)
		}
		for len(subscriptions) < len(get_all_topics.Topics) {
			subscriptions = append(subscriptions, "subscribe")
		}

		// ui.SubscribedList.Clear()
		// ui.TopicList.Clear()
		// client_app.Suspend(func() {
		// 	fmt.Printf("all topics:%s\n", get_all_topics.Topics)
		// })
		for topic_id := range get_all_topics.Topics {
			current_topic := get_all_topics.Topics[topic_id]
			if t == nil {
				t = current_topic
			}

			if current_topic.Id-1 >= int64(ui.SubscribedList.GetItemCount()) {
				ui.SubscribedList.InsertItem(int(current_topic.Id)-1, subscriptions[current_topic.Id-1], "", 0, func() {
					if subscriptions[current_topic.Id-1] == "subscribe" {
						// Pridobi assigned node za naročnino (load balancing)
						subNode, err := mbWrite.GetSubscriptionNode(ctx, &pb.SubscriptionNodeRequest{
							UserId:  u.Id,
							TopicId: []int64{current_topic.Id},
						})
						if err != nil {
							log.Fatal(err)
						}

						ui.SubscribedList.SetItemText(int(current_topic.Id)-1, "subscribed", "")
						subscriptions[current_topic.Id-1] = "subscribed"

						capturedTopic := current_topic
						go func() {
							// Poveži se na assigned node
							conn, err := grpc.Dial(subNode.Node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
							if err != nil {
								log.Printf("Subscription connection failed: %v", err)
								return
							}
							defer conn.Close()

							subClient := pb.NewMessageBoardClient(conn)

							// Pridobi zadnji message ID, da preskočimo stara sporočila
							lastMsgID := int64(0)
							msgs, _ := mbRead.GetMessages(ctx, &pb.GetMessagesRequest{TopicId: capturedTopic.Id})
							if len(msgs.Messages) > 0 {
								lastMsgID = msgs.Messages[len(msgs.Messages)-1].Id
							}

							stream, err := subClient.SubscribeTopic(ctx, &pb.SubscribeTopicRequest{
								TopicId:        []int64{capturedTopic.Id},
								UserId:         u.Id,
								FromMessageId:  lastMsgID,
								SubscribeToken: subNode.SubscribeToken,
							})
							if err != nil {
								log.Printf("SubscribeTopic failed: %v", err)
								return
							}

							skipCount := 0
							for {
								if subscriptions[capturedTopic.Id-1] == "subscribe" {
									return
								}
								event, err := stream.Recv()
								if err != nil {
									log.Printf("Stream error: %v", err)
									return
								}

								if event.Message.Id <= lastMsgID {
									skipCount++
									continue
								}

								client_app.QueueUpdateDraw(func() {
									currentText := ui.SubscriptionEvents.GetText(false)

									switch event.Op {
									case pb.OpType_OP_POST:
										eventMsg := fmt.Sprintf("[green][POST][white] %s | msg#%d | user=%d | %q | [yellow]♥%d[white]\n",
											capturedTopic.Name,
											event.Message.Id,
											event.Message.UserId,
											event.Message.Text,
											event.Message.Likes)
										ui.SubscriptionEvents.SetText(currentText + eventMsg)
										ui.SubscriptionEvents.ScrollToEnd()

									case pb.OpType_OP_UPDATE:
										eventMsg := fmt.Sprintf("[yellow][UPDATE][white] %s | msg#%d | user=%d | %q | [yellow]♥%d[white]\n",
											capturedTopic.Name,
											event.Message.Id,
											event.Message.UserId,
											event.Message.Text,
											event.Message.Likes)
										ui.SubscriptionEvents.SetText(currentText + eventMsg)
										ui.SubscriptionEvents.ScrollToEnd()

									case pb.OpType_OP_DELETE:
										eventMsg := fmt.Sprintf("[red][DELETE][white] %s | msg#%d deleted\n",
											capturedTopic.Name,
											event.Message.Id)
										ui.SubscriptionEvents.SetText(currentText + eventMsg)
										ui.SubscriptionEvents.ScrollToEnd()

									case pb.OpType_OP_LIKE:
										// Poišči in posodobi obstoječe sporočilo
										msgPattern := fmt.Sprintf("msg#%d", event.Message.Id)
										lines := []string{}
										for _, line := range strings.Split(currentText, "\n") {
											if strings.Contains(line, msgPattern) {
												// Posodobi likes count v tej vrstici
												re := regexp.MustCompile(`♥\d+`)
												line = re.ReplaceAllString(line, fmt.Sprintf("♥%d", event.Message.Likes))
											}
											lines = append(lines, line)
										}
										ui.SubscriptionEvents.SetText(strings.Join(lines, "\n"))
									}
								})
							}
						}()
					} else {
						ui.SubscribedList.SetItemText(int(current_topic.Id)-1, "subscribe", "")
						subscriptions[current_topic.Id-1] = "subscribe"
					}
				}).SetSelectedFocusOnly(true)
			}

			if current_topic.Id-1 >= int64(ui.TopicList.GetItemCount()) {
				ui.TopicList.InsertItem(int(current_topic.Id)-1, current_topic.Name, "", 0, func() {
					t = current_topic
					reloadMessages(t, u)
				})
			}
		}
	}

	reloadTopics()

	ui.TopicForm.GetButton(0).SetSelectedFunc(func() {
		if item := ui.TopicForm.GetFormItem(0); item != nil {
			text := item.(*tview.InputField).GetText()
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

	go func() {
		for {
			if u != nil {
				reloadTopics()
				if t != nil {
					reloadMessages(t, u)
					client_app.Draw()
				}
			}
			time.Sleep(1 * time.Second)
		}
	}()

	err = client_app.SetRoot(ui.Grid, true).EnableMouse(true).Run()
	if err != nil {
		log.Fatal("Create box:", err)
	}
}
