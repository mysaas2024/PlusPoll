import jwt from "jsonwebtoken";
import { logger } from "../config/logger.js";

export const middleware = (req, res, next) => {
  const authorization = req.headers.authorization;
  const [scheme, token, extra] = authorization?.split(" ") || [];
  if (scheme !== "Bearer" || !token || extra) {
    return res.status(401).json({ success: false, message: "Authentication required." });
  }
  if (!process.env.JWT_SECRET) {
    logger.error("JWT_SECRET is not configured");
    return res.status(500).json({ success: false, message: "Authentication is unavailable." });
  }
  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    if (!decoded || typeof decoded !== "object" || typeof decoded.id !== "string") {
      return res.status(401).json({ success: false, message: "Invalid token." });
    }
    req.user = decoded;
    return next();
  } catch (error) {
    return res.status(401).json({ success: false, message: "Invalid token." });
  }
};