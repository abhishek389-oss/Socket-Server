import express from "express";
import dotenv from "dotenv";
dotenv.config();
import http from "http";
import mongoose from "mongoose";
import { Server } from "socket.io";
import cors from "cors";
import User from "./models/user.model.js";

const app = express();

const allowedOrigins = [
  "https://rydex-lemon-nine.vercel.app",
  "http://localhost:3000"
];

app.use(
  cors({
    origin: allowedOrigins,
    credentials: true,
    methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
  })
);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const port = process.env.PORT || 8000;
const mongodbUrl = process.env.MONGODB_URL;

const connectDB = async () => {
  try {
    await mongoose.connect(mongodbUrl);
    console.log("DB connected successfully");
  } catch (error) {
    console.log("DB connection error:", error);
  }
};

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
    credentials: true
  },
  pingTimeout: 20000,
  pingInterval: 10000,
  transports: ["websocket", "polling"]
});

const userSocketMap = new Map();
const socketUserMap = new Map();

app.post("/emit", async (req, res) => {
  try {
    const { event, userId, data } = req.body;

    if (userId === "admin") {
      io.to("admin_room").emit(event, data);
      console.log(`[EMIT SUCCESS] Event "${event}" sent to admin_room`);
      return res.status(200).json({ success: true });
    }

    if (!userId) {
      return res.status(400).json({ success: false, message: "User ID is required" });
    }

    const strUserId = String(userId);

    let socketId = userSocketMap.get(strUserId);

    if (!socketId) {
      const user = await User.findById(userId).select("socketId");
      if (user?.socketId) {
        socketId = user.socketId;
        userSocketMap.set(strUserId, socketId);
      }
    }

    if (socketId) {
      io.to(socketId).emit(event, data);
      console.log(`[EMIT SUCCESS] Event "${event}" sent to user ${userId} on socket ${socketId}`);
      return res.status(200).json({ success: true });
    } else {
      console.log(`[EMIT FAIL] User ${userId} is offline or not registered.`);
      return res.status(200).json({ success: false, message: "User not online" });
    }
  } catch (error) {
    console.log("Emit route error:", error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  socket.on("join_admin", () => {
    socket.join("admin_room");
    console.log(`Admin joined admin_room: ${socket.id}`);
  });

  socket.on("identity", async (userId) => {
    try {
      if (!userId) return;
      const strUserId = String(userId);
      socket.userId = strUserId;

      userSocketMap.set(strUserId, socket.id);
      socketUserMap.set(socket.id, strUserId);

      User.findByIdAndUpdate(strUserId, {
        socketId: socket.id,
        isOnline: true,
      }).catch((e) => console.log("DB update error on identity:", e));

      console.log(`User ${strUserId} registered with socket ${socket.id}`);
    } catch (error) {
      console.log("Identity error:", error);
    }
  });

  socket.on("join_ride", (bookingId) => {
    if (!bookingId) return;
    const room = `ride_${bookingId}`;
    socket.join(room);
    console.log(`Socket ${socket.id} joined room ${room}`);
  });

  socket.on("driver_location_update", (data) => {
    if (data?.bookingId) {
      socket.to(`ride_${data.bookingId}`).emit("driver_location_update", data);
    }
  });

  socket.on("ride_started", (data) => {
    if (data?.bookingId) {
      io.to(`ride_${data.bookingId}`).emit("ride_started", data);
    }
  });

  socket.on("ride_completed", (data) => {
    if (data?.bookingId) {
      io.to(`ride_${data.bookingId}`).emit("ride_completed", data);
    }
  });

  socket.on("chat_message", (messageData) => {
    if (messageData?.bookingId) {
      const room = `ride_${messageData.bookingId}`;
      socket.to(room).emit("chat_message", messageData);
      console.log(`[CHAT] Relayed message in ${room}`);
    }
  });

  socket.on("update-location", async ({ userId, location }) => {
    try {
      if (!userId || !location) return;
      await User.findByIdAndUpdate(userId, { location: location });
    } catch (error) {
      console.log("update-location error:", error);
    }
  });

  socket.on("disconnect", async () => {
    try {
      const userId = socket.userId || socketUserMap.get(socket.id);
      if (userId) {
        userSocketMap.delete(userId);
        socketUserMap.delete(socket.id);

        User.findByIdAndUpdate(userId, {
          socketId: null,
          isOnline: false,
        }).catch((e) => console.log("DB update error on disconnect:", e));

        console.log(`User ${userId} disconnected`);
      }
    } catch (error) {
      console.log("Disconnect error:", error);
    }
  });
});

server.listen(port, () => {
  console.log(`Server running on port ${port}`);
  connectDB();
});