import { drizzle } from 'drizzle-orm/postgres-js';
import postgres from 'postgres';
import { users, campuses, events, chatSessions, type User, type InsertUser, type Campus, type InsertCampus, type Event, type InsertEvent, type ChatSession, type InsertChatSession } from "@shared/schema";
import { eq, and, gte, lte, desc, asc, count } from 'drizzle-orm';

const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  throw new Error('DATABASE_URL environment variable is required');
}

const client = postgres(connectionString);
const db = drizzle(client);

export class SupabaseStorage {
  // User operations
  async getUser(id: number): Promise<User | undefined> {
    const result = await db.select().from(users).where(eq(users.id, id)).limit(1);
    return result[0];
  }

  async getUserByEmail(email: string): Promise<User | undefined> {
    const result = await db.select().from(users).where(eq(users.email, email)).limit(1);
    return result[0];
  }

  async createUser(userData: InsertUser): Promise<User> {
    const result = await db.insert(users).values(userData).returning();
    return result[0];
  }

  // Campus operations
  async getCampus(id: number): Promise<Campus | undefined> {
    const result = await db.select().from(campuses).where(eq(campuses.id, id)).limit(1);
    return result[0];
  }

  async getAllCampuses(): Promise<Campus[]> {
    return await db.select().from(campuses).orderBy(asc(campuses.name));
  }

  async createCampus(campusData: InsertCampus): Promise<Campus> {
    const result = await db.insert(campuses).values(campusData).returning();
    return result[0];
  }

  async updateCampus(id: number, updates: Partial<InsertCampus>): Promise<Campus | undefined> {
    const result = await db.update(campuses).set(updates).where(eq(campuses.id, id)).returning();
    return result[0];
  }

  // Event operations
  async getEvent(id: number): Promise<Event | undefined> {
    const result = await db.select().from(events).where(eq(events.id, id)).limit(1);
    return result[0];
  }

  async getEventsByCampus(campusId: number): Promise<Event[]> {
    return await db.select().from(events).where(eq(events.campusId, campusId)).orderBy(desc(events.dateTime));
  }

  async getEventsInDateRange(campusId: number, startDate: Date, endDate: Date): Promise<Event[]> {
    return await db.select().from(events).where(
      and(
        eq(events.campusId, campusId),
        gte(events.dateTime, startDate),
        lte(events.dateTime, endDate)
      )
    ).orderBy(asc(events.dateTime));
  }

  async createEvent(eventData: InsertEvent): Promise<Event> {
    const processedData = {
      ...eventData,
      dateTime: new Date(eventData.dateTime),
      endDateTime: eventData.endDateTime ? new Date(eventData.endDateTime) : null,
    };
    const result = await db.insert(events).values(processedData).returning();
    return result[0];
  }

  async updateEvent(id: number, updates: Partial<InsertEvent>): Promise<Event | undefined> {
    const processedUpdates = {
      ...updates,
      dateTime: updates.dateTime ? new Date(updates.dateTime) : undefined,
      endDateTime: updates.endDateTime ? new Date(updates.endDateTime) : undefined,
    };
    const result = await db.update(events).set(processedUpdates).where(eq(events.id, id)).returning();
    return result[0];
  }

  async deleteEvent(id: number): Promise<boolean> {
    const result = await db.delete(events).where(eq(events.id, id)).returning();
    return result.length > 0;
  }

  // Chat operations
  async createChatSession(chatData: InsertChatSession): Promise<ChatSession> {
    const result = await db.insert(chatSessions).values(chatData).returning();
    return result[0];
  }

  async getChatHistory(userId: number, limit: number = 10): Promise<ChatSession[]> {
    return await db.select().from(chatSessions)
      .where(eq(chatSessions.userId, userId))
      .orderBy(desc(chatSessions.createdAt))
      .limit(limit);
  }

  // Analytics
  async getEventTypeDistribution(campusId: number): Promise<{ type: string; count: number }[]> {
    const result = await db.select({
      type: events.programType,
      count: count()
    })
    .from(events)
    .where(eq(events.campusId, campusId))
    .groupBy(events.programType);
    
    return result.map(row => ({
      type: row.type,
      count: Number(row.count)
    }));
  }

  async getMonthlyParticipation(campusId: number, year: number): Promise<{ month: string; participants: number }[]> {
    const dbEvents = await db.select({
      dateTime: events.dateTime,
      participantCount: events.participantCount
    })
    .from(events)
    .where(eq(events.campusId, campusId));
    
    const monthlyData = dbEvents
      .filter((event) => new Date(event.dateTime).getFullYear() === year)
      .reduce((acc: Record<string, number>, event) => {
        const month = new Date(event.dateTime).toLocaleString('default', { month: 'long' });
        acc[month] = (acc[month] || 0) + (event.participantCount || 0);
        return acc;
      }, {} as Record<string, number>);
    
    return Object.entries(monthlyData).map(([month, participants]) => ({ month, participants }));
  }

  async getTopRatedEvents(campusId: number, limit: number = 5): Promise<Event[]> {
    return await db.select().from(events)
      .where(eq(events.campusId, campusId))
      .orderBy(desc(events.rating))
      .limit(limit);
  }
} 