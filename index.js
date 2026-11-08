import express from "express";
import dotenv from "dotenv";
dotenv.config();
import http from "http";
import mongoose from "mongoose";
import { Server } from "socket.io";
import cors from "cors";
import User from "./models/user.model.js";

const app = express();

app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const port = process.env.PORT || 8000;
const mongodbUrl = process.env.MONGODB_URL;

const connectDB = async () => {
  try {
    await mongoose.connect(mongodbUrl);
    console.log("DB connected");
  } catch (error) {
    console.log("Db error", error);
  }
};

const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

app.post("/emit", async (req, res) => {
  try {
    const { event, userId, data } = req.body;

    if (!userId) {
      return res.status(400).json({ success: false, message: "User ID is required" });
    }

    const user = await User.findById(userId);

    if (user && user.socketId) {
      io.to(user.socketId).emit(event, data);
      console.log(`Event "${event}" emitted to user: ${userId} on socket: ${user.socketId}`);
    } else {
      console.log(`User ${userId} not online or socketId missing.`);
    }

    return res.status(200).json({ success: true });
  } catch (error) {
    console.log("Emit error:", error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  socket.on("identity", async (userId) => {
    try {
      if (!userId) return;
      socket.userId = userId;
      await User.findByIdAndUpdate(userId, {
        socketId: socket.id,
        isOnline: true,
      });
      console.log(`User ${userId} registered with socket ${socket.id}`);
    } catch (error) {
      console.log("Identity error:", error);
    }
  });

  socket.on("disconnect", async () => {
    try {
      if (!socket.userId) return;
      await User.findByIdAndUpdate(socket.userId, {
        socketId: null,
        isOnline: false,
      });
      console.log(`User ${socket.userId} disconnected`);
    } catch (error) {
      console.log("Disconnect error:", error);
    }
  });

  socket.on("update-location", async ({ userId, location }) => {
    try {
      if (!userId || !location) return;
      await User.findByIdAndUpdate(userId, {
        location: location,
      });
    } catch (error) {
      console.log("update-location error:", error);
    }
  });

  socket.on("join_ride", (bookingId) => {
    socket.join(`ride_${bookingId}`);
    console.log(`Socket ${socket.id} joined ride_${bookingId}`);
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
      io.to(`ride_${messageData.bookingId}`).emit("chat_message", messageData);
    }
  });
});

server.listen(port, () => {
  console.log(`Server started on port ${port}`);
  connectDB();
});